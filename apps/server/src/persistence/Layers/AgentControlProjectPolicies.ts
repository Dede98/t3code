import { AgentControlProjectPolicy, IsoDateTime, PositiveInt, ProjectId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { PersistenceSqlError } from "../Errors.ts";
import { AgentControlProjectAvailabilityLive } from "./AgentControlProjectAvailability.ts";
import { AgentControlProjectAvailability } from "../Services/AgentControlProjectAvailability.ts";
import {
  AgentControlProjectPolicyConflictError,
  AgentControlProjectPolicyCorruptError,
  AgentControlProjectPolicyRecord,
  AgentControlProjectPolicyRepository,
  type AgentControlProjectPolicyRepositoryShape,
  AgentControlProjectPolicyProjectUnavailableError,
  AgentControlProjectPolicyValidationError,
  ClearAgentControlProjectPolicyInput,
  SetAgentControlProjectPolicyInput,
} from "../Services/AgentControlProjectPolicies.ts";

const PersistedAgentControlProjectPolicyRow = Schema.Struct({
  projectId: ProjectId,
  policy: Schema.fromJsonString(AgentControlProjectPolicy),
  revision: PositiveInt,
  updatedAt: IsoDateTime,
});

const decodePersistedRow = Schema.decodeUnknownEffect(PersistedAgentControlProjectPolicyRow);
const decodeSetInput = Schema.decodeUnknownEffect(SetAgentControlProjectPolicyInput);
const decodeClearInput = Schema.decodeUnknownEffect(ClearAgentControlProjectPolicyInput);
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
  const projectAvailability = yield* AgentControlProjectAvailability;

  const ensureProjectAvailable: AgentControlProjectPolicyRepositoryShape["ensureProjectAvailable"] =
    (projectId) =>
      projectAvailability.ensureAvailable(projectId).pipe(
        Effect.mapError((error) =>
          error._tag === "AgentControlProjectUnavailableError"
            ? new AgentControlProjectPolicyProjectUnavailableError({
                projectId: error.projectId,
                reason: error.reason,
              })
            : sqlError("AgentControlProjectPolicyRepository.ensureProjectAvailable:query", error),
        ),
      );

  const readProjectPolicy = (projectId: ProjectId, operation: string) =>
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
      `.pipe(Effect.mapError((cause) => sqlError(operation, cause)));

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

  const getProjectPolicy: AgentControlProjectPolicyRepositoryShape["getProjectPolicy"] = (
    projectId,
  ) => readProjectPolicy(projectId, "AgentControlProjectPolicyRepository.getProjectPolicy:query");

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

      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* ensureProjectAvailable(validated.projectId);
            const existing = yield* readProjectPolicy(
              validated.projectId,
              "AgentControlProjectPolicyRepository.setProjectPolicy:readCurrent",
            );
            const actualRevision = Option.match(existing, {
              onNone: () => null,
              onSome: (record) => record.revision,
            });
            if (validated.expectedRevision !== (actualRevision ?? 0)) {
              return yield* new AgentControlProjectPolicyConflictError({
                projectId: validated.projectId,
                expectedRevision: validated.expectedRevision,
                actualRevision,
              });
            }

            const changedRows = Option.isNone(existing)
              ? yield* sql<{ readonly revision: number }>`
                  INSERT INTO agent_control_project_policies (
                    project_id,
                    policy_json,
                    revision,
                    updated_at
                  ) VALUES (
                    ${validated.projectId},
                    ${policyJson},
                    1,
                    ${updatedAt}
                  )
                  RETURNING revision
                `.pipe(
                  Effect.mapError((cause) =>
                    sqlError("AgentControlProjectPolicyRepository.setProjectPolicy:insert", cause),
                  ),
                )
              : yield* sql<{ readonly revision: number }>`
                  UPDATE agent_control_project_policies
                  SET
                    policy_json = ${policyJson},
                    revision = revision + 1,
                    updated_at = ${updatedAt}
                  WHERE project_id = ${validated.projectId}
                    AND revision = ${validated.expectedRevision}
                  RETURNING revision
                `.pipe(
                  Effect.mapError((cause) =>
                    sqlError("AgentControlProjectPolicyRepository.setProjectPolicy:update", cause),
                  ),
                );

            if (changedRows.length === 0) {
              return yield* new AgentControlProjectPolicyConflictError({
                projectId: validated.projectId,
                expectedRevision: validated.expectedRevision,
                actualRevision,
              });
            }

            return {
              projectId: validated.projectId,
              policy: validated.policy,
              revision: nextRevision,
              updatedAt,
            } satisfies AgentControlProjectPolicyRecord;
          }),
        )
        .pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(
              sqlError("AgentControlProjectPolicyRepository.setProjectPolicy:transaction", cause),
            ),
          ),
        );
    });

  const clearProjectPolicy: AgentControlProjectPolicyRepositoryShape["clearProjectPolicy"] = (
    input,
  ) =>
    Effect.gen(function* () {
      const validated = yield* decodeClearInput(input).pipe(
        Effect.mapError(
          (cause) =>
            new AgentControlProjectPolicyValidationError({
              projectId: String(input.projectId),
              operation: "clearProjectPolicy",
              issue: cause.message,
              cause,
            }),
        ),
      );

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* ensureProjectAvailable(validated.projectId);
            const existing = yield* readProjectPolicy(
              validated.projectId,
              "AgentControlProjectPolicyRepository.clearProjectPolicy:readCurrent",
            );
            const actualRevision = Option.match(existing, {
              onNone: () => null,
              onSome: (record) => record.revision,
            });
            if (actualRevision === null || actualRevision !== validated.expectedRevision) {
              return yield* new AgentControlProjectPolicyConflictError({
                projectId: validated.projectId,
                expectedRevision: validated.expectedRevision,
                actualRevision,
              });
            }

            const changedRows = yield* sql<{ readonly revision: unknown }>`
              DELETE FROM agent_control_project_policies
              WHERE project_id = ${validated.projectId}
                AND revision = ${validated.expectedRevision}
              RETURNING revision
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlProjectPolicyRepository.clearProjectPolicy:delete", cause),
              ),
            );
            if (changedRows.length === 0) {
              return yield* new AgentControlProjectPolicyConflictError({
                projectId: validated.projectId,
                expectedRevision: validated.expectedRevision,
                actualRevision,
              });
            }
          }),
        )
        .pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(
              sqlError("AgentControlProjectPolicyRepository.clearProjectPolicy:transaction", cause),
            ),
          ),
        );
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
    ensureProjectAvailable,
    getProjectPolicy,
    setProjectPolicy,
    clearProjectPolicy,
    deleteProjectPolicy,
  } satisfies AgentControlProjectPolicyRepositoryShape;
});

export const AgentControlProjectPolicyRepositoryLive = Layer.effect(
  AgentControlProjectPolicyRepository,
  makeAgentControlProjectPolicyRepository,
).pipe(Layer.provide(AgentControlProjectAvailabilityLive));
