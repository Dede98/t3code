import { IsoDateTime, NonNegativeInt, PositiveInt, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
  AgentControlTaskReconcileConflictError,
} from "../../Errors.ts";
import {
  AgentControlTaskReconcileState,
  AgentControlTaskReconcileStateRepository,
  AgentControlTaskReconcileStatus,
  type AgentControlTaskReconcileStateRepositoryShape,
} from "../Services/AgentControlTaskReconcileState.ts";

const ReconcileRow = Schema.Struct({
  projectId: ProjectId,
  targetSequence: PositiveInt,
  lastCompletedSequence: NonNegativeInt,
  revision: PositiveInt,
  status: AgentControlTaskReconcileStatus,
  updatedAt: IsoDateTime,
});
const decodeRow = Schema.decodeUnknownEffect(ReconcileRow);
const decodeSequence = Schema.decodeUnknownEffect(PositiveInt);
const decodeRevision = Schema.decodeUnknownEffect(NonNegativeInt);
const decodeUpdatedAt = Schema.decodeUnknownEffect(IsoDateTime);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });

const makeRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const get: AgentControlTaskReconcileStateRepositoryShape["get"] = (projectId) =>
    sql<Record<string, unknown>>`
      SELECT project_id AS "projectId", target_sequence AS "targetSequence",
             last_completed_sequence AS "lastCompletedSequence", revision,
             status, updated_at AS "updatedAt"
      FROM agent_control_task_reconcile_states
      WHERE project_id = ${projectId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlTaskReconcileStateRepository.get", cause)),
      Effect.flatMap((rows) => {
        const row = rows[0];
        if (row === undefined) return Effect.succeed(Option.none());
        return decodeRow(row).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlTaskReconcileStateRepository.get", cause),
          ),
          Effect.flatMap((decoded) =>
            decoded.lastCompletedSequence <= decoded.targetSequence
              ? Effect.succeed(Option.some({ schemaVersion: 1 as const, ...decoded }))
              : Effect.fail(
                  decodeError(
                    "AgentControlTaskReconcileStateRepository.get:invariant",
                    new Error("reconcile sequence invariant"),
                  ),
                ),
          ),
        );
      }),
    );

  const actualRevision = (projectId: ProjectId) =>
    sql<{ readonly revision: unknown }>`
      SELECT COALESCE(MAX(revision), 0) AS revision
      FROM agent_control_task_reconcile_states
      WHERE project_id = ${projectId}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlTaskReconcileStateRepository.actualRevision", cause),
      ),
      Effect.flatMap((rows) =>
        decodeRevision(rows[0]?.revision).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlTaskReconcileStateRepository.actualRevision", cause),
          ),
        ),
      ),
    );

  const failConflict = Effect.fn("AgentControlTaskReconcileStateRepository.failConflict")(
    function* (projectId: ProjectId, expectedRevision: number) {
      return yield* new AgentControlTaskReconcileConflictError({
        projectId,
        expectedRevision,
        actualRevision: yield* actualRevision(projectId),
      });
    },
  );

  const begin: AgentControlTaskReconcileStateRepositoryShape["begin"] = (
    projectId,
    rawTargetSequence,
    rawUpdatedAt,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const targetSequence = yield* decodeSequence(rawTargetSequence).pipe(
            Effect.mapError((cause) =>
              decodeError("AgentControlTaskReconcileStateRepository.begin:sequence", cause),
            ),
          );
          const updatedAt = yield* decodeUpdatedAt(rawUpdatedAt).pipe(
            Effect.mapError((cause) =>
              decodeError("AgentControlTaskReconcileStateRepository.begin:updatedAt", cause),
            ),
          );
          const current = yield* get(projectId);
          if (Option.isNone(current)) {
            const rows = yield* sql`
            INSERT INTO agent_control_task_reconcile_states (
              project_id, target_sequence, last_completed_sequence,
              revision, status, updated_at
            ) VALUES (${projectId}, ${targetSequence}, 0, 1, 'reconciling', ${updatedAt})
            ON CONFLICT (project_id) DO NOTHING
            RETURNING project_id
          `;
            if (rows.length !== 1) return yield* failConflict(projectId, 0);
            return {
              schemaVersion: 1,
              projectId,
              targetSequence,
              lastCompletedSequence: 0,
              revision: 1,
              status: "reconciling",
              updatedAt,
            } satisfies AgentControlTaskReconcileState;
          }
          const previous = current.value;
          if (
            targetSequence < previous.targetSequence ||
            targetSequence < previous.lastCompletedSequence
          ) {
            return yield* new AgentControlTaskReconcileConflictError({
              projectId,
              expectedRevision: previous.revision,
              actualRevision: previous.revision,
            });
          }
          const nextRevision = previous.revision + 1;
          const rows = yield* sql`
          UPDATE agent_control_task_reconcile_states
          SET target_sequence = ${targetSequence}, revision = ${nextRevision},
              status = 'reconciling', updated_at = ${updatedAt}
          WHERE project_id = ${projectId} AND revision = ${previous.revision}
          RETURNING project_id
        `;
          if (rows.length !== 1) {
            return yield* failConflict(projectId, previous.revision);
          }
          return {
            ...previous,
            targetSequence,
            revision: nextRevision,
            status: "reconciling" as const,
            updatedAt,
          } satisfies AgentControlTaskReconcileState;
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(
            sqlError("AgentControlTaskReconcileStateRepository.begin:transaction", cause),
          ),
        ),
      );

  const transition = Effect.fn("AgentControlTaskReconcileStateRepository.transition")(function* (
    projectId: ProjectId,
    rawTargetSequence: number,
    rawExpectedRevision: number,
    rawUpdatedAt: string,
    status: "completed" | "recovery-required",
  ) {
    const targetSequence = yield* decodeSequence(rawTargetSequence).pipe(
      Effect.mapError((cause) =>
        decodeError("AgentControlTaskReconcileStateRepository.transition:sequence", cause),
      ),
    );
    const expectedRevision = yield* decodeRevision(rawExpectedRevision).pipe(
      Effect.mapError((cause) =>
        decodeError("AgentControlTaskReconcileStateRepository.transition:revision", cause),
      ),
    );
    const updatedAt = yield* decodeUpdatedAt(rawUpdatedAt).pipe(
      Effect.mapError((cause) =>
        decodeError("AgentControlTaskReconcileStateRepository.transition:updatedAt", cause),
      ),
    );
    const current = yield* get(projectId);
    if (
      Option.isNone(current) ||
      current.value.revision !== expectedRevision ||
      current.value.targetSequence !== targetSequence ||
      current.value.status !== "reconciling" ||
      targetSequence < current.value.lastCompletedSequence
    ) {
      return yield* failConflict(projectId, expectedRevision);
    }
    const nextRevision = expectedRevision + 1;
    const lastCompletedSequence =
      status === "completed" ? targetSequence : current.value.lastCompletedSequence;
    const rows = yield* sql`
        UPDATE agent_control_task_reconcile_states
        SET last_completed_sequence = ${lastCompletedSequence},
            revision = ${nextRevision}, status = ${status}, updated_at = ${updatedAt}
        WHERE project_id = ${projectId} AND revision = ${expectedRevision}
          AND target_sequence = ${targetSequence} AND status = 'reconciling'
        RETURNING project_id
      `;
    if (rows.length !== 1) return yield* failConflict(projectId, expectedRevision);
    return {
      ...current.value,
      lastCompletedSequence,
      revision: nextRevision,
      status,
      updatedAt,
    };
  });

  const markRecoveryRequired: AgentControlTaskReconcileStateRepositoryShape["markRecoveryRequired"] =
    (projectId, targetSequence, expectedRevision, updatedAt) =>
      sql
        .withTransaction(
          transition(projectId, targetSequence, expectedRevision, updatedAt, "recovery-required"),
        )
        .pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(
              sqlError(
                "AgentControlTaskReconcileStateRepository.markRecoveryRequired:transaction",
                cause,
              ),
            ),
          ),
        );

  const complete: AgentControlTaskReconcileStateRepositoryShape["complete"] = (
    projectId,
    targetSequence,
    expectedRevision,
    updatedAt,
  ) =>
    sql
      .withTransaction(
        transition(projectId, targetSequence, expectedRevision, updatedAt, "completed"),
      )
      .pipe(
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(
            sqlError("AgentControlTaskReconcileStateRepository.complete:transaction", cause),
          ),
        ),
      );

  return AgentControlTaskReconcileStateRepository.of({
    get,
    begin,
    markRecoveryRequired,
    complete,
  });
});

export const layer = Layer.effect(AgentControlTaskReconcileStateRepository, makeRepository);
