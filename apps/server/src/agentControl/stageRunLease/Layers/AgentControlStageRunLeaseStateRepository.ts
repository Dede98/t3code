import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlStageRunLeaseState,
  AgentControlStageRunLeaseStatus,
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
import { validateAgentControlStageRunLeaseState } from "../invariant.ts";
import {
  AgentControlStageRunLeaseStateRepository,
  type AgentControlStageRunLeaseEnumerationEntry,
  type AgentControlStageRunLeaseStateRepositoryShape,
} from "../Services/AgentControlStageRunLeaseStateRepository.ts";

const StateRow = Schema.Struct({
  state: Schema.fromJsonString(AgentControlStageRunLeaseState),
  leaseId: AgentControlStageRunLeaseId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  holderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
  status: AgentControlStageRunLeaseStatus,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: Schema.String,
  acquiredAt: IsoDateTime,
  renewedAt: IsoDateTime,
  expiresAt: IsoDateTime,
  releasedAt: Schema.NullOr(IsoDateTime),
  revision: NonNegativeInt,
  sequence: NonNegativeInt,
});
const decodeRow = Schema.decodeUnknownEffect(StateRow);
const decodeState = Schema.decodeUnknownEffect(AgentControlStageRunLeaseState);
const encodeState = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlStageRunLeaseState),
);
const decodeLeaseId = Schema.decodeUnknownEffect(AgentControlStageRunLeaseId);
const decodeProjectId = Schema.decodeUnknownEffect(ProjectId);
const decodeTaskId = Schema.decodeUnknownEffect(AgentControlTaskId);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });

const SELECT_COLUMNS = `
  state_json AS state, lease_id AS "leaseId", project_id AS "projectId",
  task_id AS "taskId", stage_run_id AS "stageRunId", attempt_id AS "attemptId",
  holder_id AS "holderId", fence_token AS "fenceToken", status,
  task_revision AS "taskRevision", github_intake_sequence AS "githubIntakeSequence",
  source_identity_fingerprint AS "sourceIdentityFingerprint",
  acquired_at AS "acquiredAt", renewed_at AS "renewedAt",
  expires_at AS "expiresAt", released_at AS "releasedAt", revision,
  last_event_sequence AS sequence
`;

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const decodeInvariant = (row: Record<string, unknown>, operation: string) =>
    decodeRow(row).pipe(
      Effect.mapError((cause) => decodeError(operation, cause)),
      Effect.flatMap(({ state, ...columns }) => {
        if (
          state.leaseId !== columns.leaseId ||
          state.projectId !== columns.projectId ||
          state.taskId !== columns.taskId ||
          state.stageRunId !== columns.stageRunId ||
          state.attemptId !== columns.attemptId ||
          state.holderId !== columns.holderId ||
          state.fenceToken !== columns.fenceToken ||
          state.status !== columns.status ||
          state.taskRevision !== columns.taskRevision ||
          state.githubIntakeSequence !== columns.githubIntakeSequence ||
          state.sourceIdentityFingerprint !== columns.sourceIdentityFingerprint ||
          state.acquiredAt !== columns.acquiredAt ||
          state.renewedAt !== columns.renewedAt ||
          state.expiresAt !== columns.expiresAt ||
          state.releasedAt !== columns.releasedAt ||
          state.revision !== columns.revision ||
          state.sequence !== columns.sequence
        ) {
          return Effect.fail(decodeError(operation, new Error("lease projection mismatch")));
        }
        return validateAgentControlStageRunLeaseState(state).pipe(
          Effect.mapError((cause) => decodeError(operation, cause)),
        );
      }),
    );

  const get: AgentControlStageRunLeaseStateRepositoryShape["get"] = (leaseId) =>
    sql
      .unsafe<Record<string, unknown>>(
        `SELECT ${SELECT_COLUMNS}
       FROM main.agent_control_stage_run_lease_states WHERE lease_id = ?`,
        [leaseId],
      )
      .pipe(
        Effect.mapError((cause) => sqlError("AgentControlStageRunLeaseStateRepository.get", cause)),
        Effect.flatMap((rows) =>
          rows[0] === undefined
            ? Effect.succeed(Option.none())
            : decodeInvariant(rows[0], "AgentControlStageRunLeaseStateRepository.get").pipe(
                Effect.map(Option.some),
              ),
        ),
      );

  const save: AgentControlStageRunLeaseStateRepositoryShape["save"] = (
    rawState,
    expectedRevision,
  ) =>
    Effect.gen(function* () {
      const state = yield* decodeState(rawState).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlStageRunLeaseStateRepository.save:input", cause),
        ),
        Effect.flatMap((decoded) =>
          validateAgentControlStageRunLeaseState(decoded).pipe(
            Effect.mapError((cause) =>
              decodeError("AgentControlStageRunLeaseStateRepository.save:invariant", cause),
            ),
          ),
        ),
      );
      if (state.revision !== expectedRevision + 1) {
        return yield* decodeError(
          "AgentControlStageRunLeaseStateRepository.save:revision",
          new Error("lease projection revision mismatch"),
        );
      }
      const stateJson = yield* encodeState(state).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlStageRunLeaseStateRepository.save:encode", cause),
        ),
      );
      const rows =
        expectedRevision === 0
          ? yield* sql<{ readonly leaseId: unknown }>`
              INSERT INTO main.agent_control_stage_run_lease_states (
                lease_id, project_id, task_id, stage_run_id, attempt_id,
                task_revision, github_intake_sequence, source_identity_fingerprint,
                holder_id, fence_token, status, acquired_at, renewed_at,
                expires_at, released_at, state_json, revision, last_event_sequence
              ) VALUES (
                ${state.leaseId}, ${state.projectId}, ${state.taskId},
                ${state.stageRunId}, ${state.attemptId}, ${state.taskRevision},
                ${state.githubIntakeSequence}, ${state.sourceIdentityFingerprint},
                ${state.holderId}, ${state.fenceToken}, ${state.status},
                ${state.acquiredAt}, ${state.renewedAt}, ${state.expiresAt},
                ${state.releasedAt}, ${stateJson}, ${state.revision}, ${state.sequence}
              )
              ON CONFLICT (lease_id) DO NOTHING
              RETURNING lease_id AS "leaseId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlStageRunLeaseStateRepository.save:insert", cause),
              ),
            )
          : yield* sql<{ readonly leaseId: unknown }>`
              UPDATE main.agent_control_stage_run_lease_states SET
                stage_run_id = ${state.stageRunId},
                attempt_id = ${state.attemptId},
                task_revision = ${state.taskRevision},
                github_intake_sequence = ${state.githubIntakeSequence},
                source_identity_fingerprint = ${state.sourceIdentityFingerprint},
                holder_id = ${state.holderId},
                fence_token = ${state.fenceToken},
                status = ${state.status},
                acquired_at = ${state.acquiredAt},
                renewed_at = ${state.renewedAt},
                expires_at = ${state.expiresAt},
                released_at = ${state.releasedAt},
                state_json = ${stateJson},
                revision = ${state.revision},
                last_event_sequence = ${state.sequence}
              WHERE lease_id = ${state.leaseId} AND project_id = ${state.projectId}
                AND task_id = ${state.taskId} AND revision = ${expectedRevision}
              RETURNING lease_id AS "leaseId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlStageRunLeaseStateRepository.save:update", cause),
              ),
            );
      if (rows.length !== 1) {
        return yield* decodeError(
          "AgentControlStageRunLeaseStateRepository.save:conflict",
          new Error("lease projection write conflict"),
        );
      }
    });

  const listProject: AgentControlStageRunLeaseStateRepositoryShape["listProject"] = (projectId) =>
    sql
      .unsafe<Record<string, unknown>>(
        `SELECT ${SELECT_COLUMNS}
       FROM main.agent_control_stage_run_lease_states
       WHERE project_id = ?
       ORDER BY task_id ASC, lease_id ASC`,
        [projectId],
      )
      .pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlStageRunLeaseStateRepository.listProject", cause),
        ),
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) =>
            decodeInvariant(row, "AgentControlStageRunLeaseStateRepository.listProject").pipe(
              Effect.matchEffect({
                onSuccess: (state) =>
                  Effect.succeed({
                    _tag: "Valid" as const,
                    state,
                  } satisfies AgentControlStageRunLeaseEnumerationEntry),
                onFailure: () =>
                  Effect.gen(function* () {
                    const leaseId = yield* decodeLeaseId(row.leaseId).pipe(
                      Effect.option,
                      Effect.map(Option.getOrNull),
                    );
                    const rowProjectId = yield* decodeProjectId(row.projectId).pipe(
                      Effect.option,
                      Effect.map(Option.getOrNull),
                    );
                    const taskId = yield* decodeTaskId(row.taskId).pipe(
                      Effect.option,
                      Effect.map(Option.getOrNull),
                    );
                    return {
                      _tag: "Corrupt" as const,
                      leaseId,
                      projectId: rowProjectId,
                      taskId,
                    } satisfies AgentControlStageRunLeaseEnumerationEntry;
                  }),
              }),
            ),
          ),
        ),
      );

  const listAll: AgentControlStageRunLeaseStateRepositoryShape["listAll"] = sql
    .unsafe<Record<string, unknown>>(
      `SELECT ${SELECT_COLUMNS}
       FROM main.agent_control_stage_run_lease_states
       ORDER BY project_id ASC, task_id ASC, lease_id ASC`,
    )
    .pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlStageRunLeaseStateRepository.listAll", cause),
      ),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          decodeInvariant(row, "AgentControlStageRunLeaseStateRepository.listAll").pipe(
            Effect.matchEffect({
              onSuccess: (state) =>
                Effect.succeed({
                  _tag: "Valid" as const,
                  state,
                } satisfies AgentControlStageRunLeaseEnumerationEntry),
              onFailure: () =>
                Effect.gen(function* () {
                  const leaseId = yield* decodeLeaseId(row.leaseId).pipe(
                    Effect.option,
                    Effect.map(Option.getOrNull),
                  );
                  const projectId = yield* decodeProjectId(row.projectId).pipe(
                    Effect.option,
                    Effect.map(Option.getOrNull),
                  );
                  const taskId = yield* decodeTaskId(row.taskId).pipe(
                    Effect.option,
                    Effect.map(Option.getOrNull),
                  );
                  return {
                    _tag: "Corrupt" as const,
                    leaseId,
                    projectId,
                    taskId,
                  } satisfies AgentControlStageRunLeaseEnumerationEntry;
                }),
            }),
          ),
        ),
      ),
    );

  const deleteAll = sql`DELETE FROM main.agent_control_stage_run_lease_states`.pipe(
    Effect.mapError((cause) =>
      sqlError("AgentControlStageRunLeaseStateRepository.deleteAll", cause),
    ),
    Effect.asVoid,
  );

  return AgentControlStageRunLeaseStateRepository.of({
    get,
    save,
    listProject,
    listAll,
    deleteAll,
  });
});

export const layer = Layer.effect(AgentControlStageRunLeaseStateRepository, make);
