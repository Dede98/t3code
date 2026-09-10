import type { ProjectId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import type { AgentControlRunOnceError } from "./model.ts";
import { AgentControlRunOnceReadNotifications } from "./readNotifications.ts";

const hasErrorCode = Schema.is(Schema.Struct({ code: Schema.String }));

/** Observability only: this record grants no execution or recovery authority. */
export const persistRunOnceDiagnostic = Effect.fn("AgentControlRunOnce.persistDiagnostic")(
  function* (
    sql: SqlClient.SqlClient,
    projectId: ProjectId,
    failure: AgentControlRunOnceError | null,
  ) {
    // Historical recovery fixtures deliberately predate the read-side migration.
    const available = yield* sql`SELECT 1 FROM main.sqlite_schema
      WHERE type = 'table' AND name = 'agent_control_run_once_diagnostics'`;
    if (available.length === 0) return;
    if (failure === null) {
      yield* sql`DELETE FROM main.agent_control_run_once_diagnostics WHERE project_id = ${projectId}`;
    } else {
      const detail = hasErrorCode(failure.cause) ? failure.cause.code : null;
      const errorCode = detail ? `${failure.reason}: ${detail}` : failure.reason;
      yield* sql`INSERT INTO main.agent_control_run_once_diagnostics (project_id, run_id, step, error_code, updated_at)
        VALUES (${projectId}, ${failure.runId}, ${failure.step}, ${errorCode}, ${DateTime.formatIso(yield* DateTime.now)})
        ON CONFLICT(project_id) DO UPDATE SET run_id = excluded.run_id, step = excluded.step,
          error_code = excluded.error_code, updated_at = excluded.updated_at`;
    }
    const notifications = yield* AgentControlRunOnceReadNotifications;
    yield* notifications.publishProject(projectId);
  },
  Effect.catchCause((cause) => Effect.logWarning("Run-Once diagnostic write failed", { cause })),
);
