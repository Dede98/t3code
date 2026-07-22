import {
  AgentControlProjectMode,
  AgentControlProjectState,
  AgentControlProjectionCorruptError,
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
} from "../../agentControl/Errors.ts";
import {
  AGENT_CONTROL_PROJECT_MODE_PROJECTOR,
  isValidAgentControlProjectState,
} from "../../agentControl/projector.ts";
import {
  AgentControlProjectionCursor,
  AgentControlProjectionStateRepository,
  type AgentControlProjectionStateRepositoryShape,
  AgentControlProjectStateRepository,
  type AgentControlProjectStateRepositoryShape,
} from "../Services/AgentControlProjectStates.ts";

const PersistedProjectStateRow = Schema.Struct({
  projectId: ProjectId,
  mode: AgentControlProjectMode,
  pausedFromMode: Schema.NullOr(AgentControlProjectMode),
  revision: PositiveInt,
  sequence: PositiveInt,
  updatedAt: IsoDateTime,
});

const PersistedProjectionCursorRow = Schema.Struct({
  projectorName: Schema.String,
  lastAppliedSequence: NonNegativeInt,
  updatedAt: IsoDateTime,
});
const decodePersistedProjectStateRow = Schema.decodeUnknownEffect(PersistedProjectStateRow);
const decodeProjectState = Schema.decodeUnknownEffect(AgentControlProjectState);
const decodePersistedProjectionCursorRow = Schema.decodeUnknownEffect(PersistedProjectionCursorRow);
const decodeProjectionCursor = Schema.decodeUnknownEffect(AgentControlProjectionCursor);

const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });
const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_PROJECT_MODE_PROJECTOR,
  });

const makeAgentControlProjectStateRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const get: AgentControlProjectStateRepositoryShape["get"] = (projectId) =>
    sql<Record<string, unknown>>`
      SELECT
        project_id AS "projectId",
        mode,
        paused_from_mode AS "pausedFromMode",
        revision,
        last_event_sequence AS sequence,
        updated_at AS "updatedAt"
      FROM agent_control_project_states
      WHERE project_id = ${projectId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlProjectStateRepository.get:query", cause)),
      Effect.flatMap((rows) => {
        const row = rows[0];
        if (row === undefined) return Effect.succeed(Option.none());
        return decodePersistedProjectStateRow(row).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlProjectStateRepository.get:decode", cause),
          ),
          Effect.flatMap((decoded) => {
            const state = {
              schemaVersion: 1,
              ...decoded,
            } satisfies AgentControlProjectState;
            return isValidAgentControlProjectState(state)
              ? Effect.succeed(Option.some(state))
              : Effect.fail(corrupt());
          }),
        );
      }),
    );

  const save: AgentControlProjectStateRepositoryShape["save"] = (state, expectedRevision) =>
    Effect.gen(function* () {
      const validated = yield* decodeProjectState(state).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlProjectStateRepository.save:input", cause),
        ),
      );
      if (
        !isValidAgentControlProjectState(validated) ||
        validated.revision !== expectedRevision + 1 ||
        validated.updatedAt === null
      ) {
        return yield* corrupt();
      }

      const rows =
        expectedRevision === 0
          ? yield* sql<{ readonly projectId: unknown }>`
              INSERT INTO agent_control_project_states (
                project_id,
                mode,
                paused_from_mode,
                revision,
                last_event_sequence,
                updated_at
              ) VALUES (
                ${validated.projectId},
                ${validated.mode},
                ${validated.pausedFromMode},
                ${validated.revision},
                ${validated.sequence},
                ${validated.updatedAt}
              )
              ON CONFLICT (project_id) DO NOTHING
              RETURNING project_id AS "projectId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlProjectStateRepository.save:insert", cause),
              ),
            )
          : yield* sql<{ readonly projectId: unknown }>`
              UPDATE agent_control_project_states
              SET
                mode = ${validated.mode},
                paused_from_mode = ${validated.pausedFromMode},
                revision = ${validated.revision},
                last_event_sequence = ${validated.sequence},
                updated_at = ${validated.updatedAt}
              WHERE project_id = ${validated.projectId}
                AND revision = ${expectedRevision}
              RETURNING project_id AS "projectId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlProjectStateRepository.save:update", cause),
              ),
            );

      if (rows.length !== 1) return yield* corrupt();
    });

  const deleteAll = sql`DELETE FROM agent_control_project_states`.pipe(
    Effect.mapError((cause) =>
      sqlError("AgentControlProjectStateRepository.deleteAll:query", cause),
    ),
    Effect.asVoid,
  );

  return AgentControlProjectStateRepository.of({ get, save, deleteAll });
});

const makeAgentControlProjectionStateRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const get: AgentControlProjectionStateRepositoryShape["get"] = (projectorName) =>
    sql<Record<string, unknown>>`
      SELECT
        projector_name AS "projectorName",
        last_applied_sequence AS "lastAppliedSequence",
        updated_at AS "updatedAt"
      FROM agent_control_projection_state
      WHERE projector_name = ${projectorName}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlProjectionStateRepository.get:query", cause),
      ),
      Effect.flatMap((rows) => {
        const row = rows[0];
        if (row === undefined) return Effect.succeed(Option.none());
        return decodePersistedProjectionCursorRow(row).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlProjectionStateRepository.get:decode", cause),
          ),
          Effect.map(Option.some),
        );
      }),
    );

  const advance: AgentControlProjectionStateRepositoryShape["advance"] = (
    cursor,
    expectedSequence,
  ) =>
    Effect.gen(function* () {
      const validated = yield* decodeProjectionCursor(cursor).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlProjectionStateRepository.advance:input", cause),
        ),
      );
      if (validated.lastAppliedSequence !== expectedSequence + 1) {
        return yield* corrupt();
      }
      const rows =
        expectedSequence === 0
          ? yield* sql<{ readonly projectorName: unknown }>`
              INSERT INTO agent_control_projection_state (
                projector_name,
                last_applied_sequence,
                updated_at
              ) VALUES (
                ${validated.projectorName},
                ${validated.lastAppliedSequence},
                ${validated.updatedAt}
              )
              ON CONFLICT (projector_name) DO NOTHING
              RETURNING projector_name AS "projectorName"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlProjectionStateRepository.advance:insert", cause),
              ),
            )
          : yield* sql<{ readonly projectorName: unknown }>`
              UPDATE agent_control_projection_state
              SET
                last_applied_sequence = ${validated.lastAppliedSequence},
                updated_at = ${validated.updatedAt}
              WHERE projector_name = ${validated.projectorName}
                AND last_applied_sequence = ${expectedSequence}
              RETURNING projector_name AS "projectorName"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlProjectionStateRepository.advance:update", cause),
              ),
            );
      if (rows.length !== 1) return yield* corrupt();
    });

  const deleteAll = sql`DELETE FROM agent_control_projection_state`.pipe(
    Effect.mapError((cause) =>
      sqlError("AgentControlProjectionStateRepository.deleteAll:query", cause),
    ),
    Effect.asVoid,
  );

  return AgentControlProjectionStateRepository.of({ get, advance, deleteAll });
});

export const AgentControlProjectStateRepositoryLive = Layer.effect(
  AgentControlProjectStateRepository,
  makeAgentControlProjectStateRepository,
);

export const AgentControlProjectionStateRepositoryLive = Layer.effect(
  AgentControlProjectionStateRepository,
  makeAgentControlProjectionStateRepository,
);
