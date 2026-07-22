import { AgentControlProjectPolicy, IsoDateTime, PositiveInt, ProjectId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { PersistenceSqlError } from "../Errors.ts";
import {
  AgentControlProjectPolicyConflictError,
  AgentControlProjectPolicyCorruptError,
  AgentControlProjectPolicyRecord,
  AgentControlProjectPolicyRepository,
  type AgentControlProjectPolicyRepositoryShape,
  AgentControlProjectPolicyProjectUnavailableError,
  AgentControlProjectPolicyValidationError,
  SetAgentControlProjectPolicyInput,
} from "../Services/AgentControlProjectPolicies.ts";

const PersistedAgentControlProjectPolicyRow = Schema.Struct({
  projectId: ProjectId,
  policy: Schema.fromJsonString(AgentControlProjectPolicy),
  revision: PositiveInt,
  updatedAt: IsoDateTime,
});

const ActualRevisionRow = Schema.Struct({
  revision: PositiveInt,
});

const decodePersistedRow = Schema.decodeUnknownEffect(PersistedAgentControlProjectPolicyRow);
const decodeActualRevisionRow = Schema.decodeUnknownEffect(ActualRevisionRow);
const decodeSetInput = Schema.decodeUnknownEffect(SetAgentControlProjectPolicyInput);
const encodePolicyJson = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlProjectPolicy),
);

function sqlError(operation: string, cause: unknown): PersistenceSqlError {
  return new PersistenceSqlError({
    operation,
    cause,
  });
}

function corruptPolicyError(
  projectId: ProjectId,
  cause: Schema.SchemaError,
): AgentControlProjectPolicyCorruptError {
  return new AgentControlProjectPolicyCorruptError({
    projectId,
    issue: cause.message,
    cause,
  });
}

const makeAgentControlProjectPolicyRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const getProjectPolicy: AgentControlProjectPolicyRepositoryShape["getProjectPolicy"] = (
    projectId,
  ) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        readonly projectId: unknown;
        readonly policy: unknown;
        readonly revision: unknown;
        readonly updatedAt: unknown;
      }>`
        SELECT
          project_id AS "projectId",
          policy_json AS "policy",
          revision,
          updated_at AS "updatedAt"
        FROM agent_control_project_policies
        WHERE project_id = ${projectId}
      `.pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlProjectPolicyRepository.getProjectPolicy:query", cause),
        ),
      );

      const row = rows[0];
      if (row === undefined) {
        return Option.none();
      }

      const decoded = yield* decodePersistedRow(row).pipe(
        Effect.mapError((cause) => corruptPolicyError(projectId, cause)),
        Effect.tapError((error) =>
          Effect.logWarning("agent_control.project-policy.quarantined", {
            projectId,
            error: error.message,
          }),
        ),
      );
      return Option.some(decoded);
    });

  const setProjectPolicy: AgentControlProjectPolicyRepositoryShape["setProjectPolicy"] = (input) =>
    Effect.gen(function* () {
      const validated = yield* decodeSetInput(input).pipe(
        Effect.mapError(
          (cause) =>
            new AgentControlProjectPolicyValidationError({
              projectId: String(input.projectId),
              operation: "setProjectPolicy",
              issue: cause.message,
              cause,
            }),
        ),
      );
      const policyJson = yield* encodePolicyJson(validated.policy).pipe(
        Effect.mapError(
          (cause) =>
            new AgentControlProjectPolicyValidationError({
              projectId: validated.projectId,
              operation: "setProjectPolicy",
              issue: cause.message,
              cause,
            }),
        ),
      );
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      const nextRevision = validated.expectedRevision + 1;

      const changedRows = yield* sql<{ readonly revision: number }>`
        INSERT INTO agent_control_project_policies (
          project_id,
          policy_json,
          revision,
          updated_at
        )
        SELECT
          ${validated.projectId},
          ${policyJson},
          1,
          ${updatedAt}
        FROM projection_projects
        WHERE project_id = ${validated.projectId}
          AND deleted_at IS NULL
          AND (
            ${validated.expectedRevision} = 0
            OR EXISTS (
              SELECT 1
              FROM agent_control_project_policies
              WHERE project_id = ${validated.projectId}
            )
          )
        ON CONFLICT (project_id)
        DO UPDATE SET
          policy_json = excluded.policy_json,
          revision = agent_control_project_policies.revision + 1,
          updated_at = excluded.updated_at
        WHERE agent_control_project_policies.revision = ${validated.expectedRevision}
        RETURNING revision
      `.pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlProjectPolicyRepository.setProjectPolicy:query", cause),
        ),
      );

      if (changedRows.length > 0) {
        return {
          projectId: validated.projectId,
          policy: validated.policy,
          revision: nextRevision,
          updatedAt,
        } satisfies AgentControlProjectPolicyRecord;
      }

      const projectRows = yield* sql<{ readonly deletedAt: string | null }>`
        SELECT deleted_at AS "deletedAt"
        FROM projection_projects
        WHERE project_id = ${validated.projectId}
      `.pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlProjectPolicyRepository.setProjectPolicy:readProject", cause),
        ),
      );
      const projectRow = projectRows[0];
      if (projectRow === undefined || projectRow.deletedAt !== null) {
        return yield* new AgentControlProjectPolicyProjectUnavailableError({
          projectId: validated.projectId,
          reason: projectRow === undefined ? "missing" : "deleted",
        });
      }

      const currentRows = yield* sql<{ readonly revision: unknown }>`
        SELECT revision
        FROM agent_control_project_policies
        WHERE project_id = ${validated.projectId}
      `.pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlProjectPolicyRepository.setProjectPolicy:readRevision", cause),
        ),
      );
      const currentRow = currentRows[0];
      const actualRevision =
        currentRow === undefined
          ? null
          : yield* decodeActualRevisionRow(currentRow).pipe(
              Effect.mapError((cause) => corruptPolicyError(validated.projectId, cause)),
              Effect.tapError((error) =>
                Effect.logWarning("agent_control.project-policy.quarantined", {
                  projectId: validated.projectId,
                  error: error.message,
                }),
              ),
              Effect.map((row) => row.revision),
            );

      return yield* new AgentControlProjectPolicyConflictError({
        projectId: validated.projectId,
        expectedRevision: validated.expectedRevision,
        actualRevision,
      });
    });

  const deleteProjectPolicy: AgentControlProjectPolicyRepositoryShape["deleteProjectPolicy"] = (
    projectId,
  ) =>
    sql`
      DELETE FROM agent_control_project_policies
      WHERE project_id = ${projectId}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlProjectPolicyRepository.deleteProjectPolicy:query", cause),
      ),
      Effect.asVoid,
    );

  return {
    getProjectPolicy,
    setProjectPolicy,
    deleteProjectPolicy,
  } satisfies AgentControlProjectPolicyRepositoryShape;
});

export const AgentControlProjectPolicyRepositoryLive = Layer.effect(
  AgentControlProjectPolicyRepository,
  makeAgentControlProjectPolicyRepository,
);
