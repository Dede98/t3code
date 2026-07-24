import {
  AgentControlWorktreeReservationId,
  AgentControlWorktreeReservationState,
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
import { validateAgentControlWorktreeReservationState } from "../invariant.ts";
import {
  AgentControlWorktreeStateRepository,
  type AgentControlWorktreeEnumerationEntry,
  type AgentControlWorktreeStateRepositoryShape,
} from "../Services/AgentControlWorktreeStateRepository.ts";

const PersistedRow = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  state: Schema.fromJsonString(AgentControlWorktreeReservationState),
  revision: Schema.Number,
  sequence: Schema.Number,
});
const decodeRow = Schema.decodeUnknownEffect(PersistedRow);
const decodeReservationId = Schema.decodeUnknownEffect(AgentControlWorktreeReservationId);
const decodeProjectId = Schema.decodeUnknownEffect(ProjectId);
const encodeState = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlWorktreeReservationState),
);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });
const SELECT = `
  reservation_id AS "reservationId", project_id AS "projectId",
  state_json AS state, revision, last_event_sequence AS sequence
`;

const decodeInvariant = Effect.fn("AgentControlWorktreeStateRepository.decodeInvariant")(function* (
  row: Record<string, unknown>,
  operation: string,
) {
  const decoded = yield* decodeRow(row).pipe(
    Effect.mapError((cause) => decodeError(`${operation}:decode`, cause)),
  );
  const state = yield* validateAgentControlWorktreeReservationState(decoded.state).pipe(
    Effect.mapError((cause) => decodeError(`${operation}:invariant`, cause)),
  );
  if (
    decoded.reservationId !== state.reservationId ||
    decoded.projectId !== state.projectId ||
    decoded.revision !== state.revision ||
    decoded.sequence !== state.sequence
  ) {
    return yield* decodeError(`${operation}:columns`, new Error("projection column mismatch"));
  }
  return state;
});

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const get: AgentControlWorktreeStateRepositoryShape["get"] = (reservationId) =>
    sql
      .unsafe<Record<string, unknown>>(
        `SELECT ${SELECT}
         FROM agent_control_worktree_reservation_states
         WHERE reservation_id = ?`,
        [reservationId],
      )
      .pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlWorktreeStateRepository.get:query", cause),
        ),
        Effect.flatMap((rows) => {
          const row = rows[0];
          return row === undefined
            ? Effect.succeed(Option.none())
            : decodeInvariant(row, "AgentControlWorktreeStateRepository.get").pipe(
                Effect.map(Option.some),
              );
        }),
      );

  const getByStage: AgentControlWorktreeStateRepositoryShape["getByStage"] = (input) =>
    sql
      .unsafe<Record<string, unknown>>(
        `SELECT ${SELECT}
         FROM agent_control_worktree_reservation_states
         WHERE project_id = ? AND task_id = ? AND stage_run_id = ? AND attempt_id = ?`,
        [input.projectId, input.taskId, input.stageRunId, input.attemptId],
      )
      .pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlWorktreeStateRepository.getByStage:query", cause),
        ),
        Effect.flatMap((rows) => {
          if (rows.length > 1) {
            return Effect.fail(
              decodeError(
                "AgentControlWorktreeStateRepository.getByStage:ambiguous",
                new Error("multiple reservations for canonical stage attempt"),
              ),
            );
          }
          const row = rows[0];
          return row === undefined
            ? Effect.succeed(Option.none())
            : decodeInvariant(row, "AgentControlWorktreeStateRepository.getByStage").pipe(
                Effect.map(Option.some),
              );
        }),
      );

  const save: AgentControlWorktreeStateRepositoryShape["save"] = (rawState, expectedRevision) =>
    Effect.gen(function* () {
      const state = yield* validateAgentControlWorktreeReservationState(rawState).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlWorktreeStateRepository.save:invariant", cause),
        ),
      );
      if (state.revision !== expectedRevision + 1) {
        return yield* decodeError(
          "AgentControlWorktreeStateRepository.save:revision",
          new Error("projection revision mismatch"),
        );
      }
      const stateJson = yield* encodeState(state).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlWorktreeStateRepository.save:encode", cause),
        ),
      );
      const rows =
        expectedRevision === 0
          ? yield* sql<{ readonly reservationId: unknown }>`
              INSERT INTO agent_control_worktree_reservation_states (
                reservation_id, project_id, task_id, stage_run_id, attempt_id,
                lease_id, fence_token, repository_node_id, repository_canonical_key,
                base_ref, base_commit_sha, branch_name, internal_worktree_path,
                status, attention_code, state_json, revision, last_event_sequence,
                created_at, updated_at
              ) VALUES (
                ${state.reservationId}, ${state.projectId}, ${state.taskId},
                ${state.stageRunId}, ${state.attemptId}, ${state.leaseId},
                ${state.fenceToken}, ${state.repository.repositoryNodeId},
                ${state.repository.canonicalKey}, ${state.baseRef}, ${state.baseCommitSha},
                ${state.branchName}, ${state.internalWorktreePath}, ${state.status},
                ${state.attentionCode}, ${stateJson}, ${state.revision}, ${state.sequence},
                ${state.createdAt}, ${state.updatedAt}
              )
              ON CONFLICT (reservation_id) DO NOTHING
              RETURNING reservation_id AS "reservationId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlWorktreeStateRepository.save:insert", cause),
              ),
            )
          : yield* sql<{ readonly reservationId: unknown }>`
              UPDATE agent_control_worktree_reservation_states SET
                status = ${state.status},
                attention_code = ${state.attentionCode},
                state_json = ${stateJson},
                revision = ${state.revision},
                last_event_sequence = ${state.sequence},
                updated_at = ${state.updatedAt}
              WHERE reservation_id = ${state.reservationId}
                AND project_id = ${state.projectId}
                AND revision = ${expectedRevision}
              RETURNING reservation_id AS "reservationId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlWorktreeStateRepository.save:update", cause),
              ),
            );
      if (rows.length !== 1) {
        return yield* decodeError(
          "AgentControlWorktreeStateRepository.save:conflict",
          new Error("projection CAS conflict"),
        );
      }
    });

  const listProject: AgentControlWorktreeStateRepositoryShape["listProject"] = (projectId) =>
    sql
      .unsafe<Record<string, unknown>>(
        `SELECT ${SELECT}
         FROM agent_control_worktree_reservation_states
         WHERE project_id = ?
         ORDER BY created_at ASC, reservation_id ASC`,
        [projectId],
      )
      .pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlWorktreeStateRepository.listProject:query", cause),
        ),
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) =>
            decodeInvariant(row, "AgentControlWorktreeStateRepository.listProject").pipe(
              Effect.matchEffect({
                onSuccess: (state) =>
                  Effect.succeed({
                    _tag: "Valid",
                    state,
                  } satisfies AgentControlWorktreeEnumerationEntry),
                onFailure: () =>
                  Effect.gen(function* () {
                    const reservationId = Option.getOrNull(
                      yield* Effect.option(decodeReservationId(row.reservationId)),
                    );
                    const rowProjectId = Option.getOrNull(
                      yield* Effect.option(decodeProjectId(row.projectId)),
                    );
                    return {
                      _tag: "Corrupt",
                      reservationId,
                      projectId: rowProjectId,
                    } satisfies AgentControlWorktreeEnumerationEntry;
                  }),
              }),
            ),
          ),
        ),
      );

  return AgentControlWorktreeStateRepository.of({
    get,
    getByStage,
    save,
    listProject,
    deleteAll: sql`DELETE FROM agent_control_worktree_reservation_states`.pipe(
      Effect.mapError((cause) => sqlError("AgentControlWorktreeStateRepository.deleteAll", cause)),
      Effect.asVoid,
    ),
  });
});

export const layer = Layer.effect(AgentControlWorktreeStateRepository, make);
