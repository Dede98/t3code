import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlPersistenceSqlError,
  AgentControlProjectUnavailableError,
} from "../../agentControl/Errors.ts";
import {
  AgentControlProjectAvailability,
  type AgentControlProjectAvailabilityShape,
} from "../Services/AgentControlProjectAvailability.ts";

const makeAgentControlProjectAvailability = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const ensureAvailable: AgentControlProjectAvailabilityShape["ensureAvailable"] = (projectId) =>
    Effect.gen(function* () {
      const rows = yield* sql<{ readonly deletedAt: string | null }>`
        SELECT deleted_at AS "deletedAt"
        FROM projection_projects
        WHERE project_id = ${projectId}
      `.pipe(
        Effect.mapError(
          (cause) =>
            new AgentControlPersistenceSqlError({
              operation: "AgentControlProjectAvailability.ensureAvailable",
              cause,
            }),
        ),
      );
      const project = rows[0];
      if (project === undefined || project.deletedAt !== null) {
        return yield* new AgentControlProjectUnavailableError({
          projectId,
          reason: project === undefined ? "missing" : "deleted",
        });
      }
    });

  return AgentControlProjectAvailability.of({ ensureAvailable });
});

export const AgentControlProjectAvailabilityLive = Layer.effect(
  AgentControlProjectAvailability,
  makeAgentControlProjectAvailability,
);
