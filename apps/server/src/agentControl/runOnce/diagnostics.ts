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
      // Waiting for a task terminal is successful Run-Once processing, but does
      // not resolve a failed verification transition owned by another worker.
      yield* sql`DELETE FROM main.agent_control_run_once_diagnostics
        WHERE project_id = ${projectId}
          AND (step NOT LIKE 'verification:%' OR step IS NULL OR NOT EXISTS (
            SELECT 1 FROM main.agent_control_run_once_states run
            WHERE run.run_id = agent_control_run_once_diagnostics.run_id AND run.status = 'active'
          ))`;
    } else {
      const detail = hasErrorCode(failure.cause) ? failure.cause.code : null;
      const errorCode = detail ? `${failure.reason}: ${detail}` : failure.reason;
      yield* sql`INSERT INTO main.agent_control_run_once_diagnostics (project_id, run_id, step, error_code, updated_at)
        VALUES (${projectId}, ${failure.runId}, ${failure.step}, ${errorCode}, ${DateTime.formatIso(yield* DateTime.now)})
        ON CONFLICT(project_id) DO UPDATE SET run_id = excluded.run_id, step = excluded.step,
          error_code = excluded.error_code, updated_at = excluded.updated_at
        WHERE agent_control_run_once_diagnostics.step NOT LIKE 'verification:%'
          OR agent_control_run_once_diagnostics.step IS NULL OR NOT EXISTS (
            SELECT 1 FROM main.agent_control_run_once_states run
            WHERE run.run_id = agent_control_run_once_diagnostics.run_id AND run.status = 'active'
          )`;
    }
    const notifications = yield* AgentControlRunOnceReadNotifications;
    yield* notifications.publishProject(projectId);
  },
);

/** A transition's diagnostic survives ordinary Run-Once waits until that same
 * implementation result materializes verification, or its run ends. */
export const persistVerificationRunOnceDiagnostic = Effect.fn(
  "AgentControlRunOnce.persistVerificationDiagnostic",
)(
  function* (
    sql: SqlClient.SqlClient,
    implementationResultEvidenceId: string,
    failure: { readonly operation: string; readonly reason: string } | null,
  ) {
    const available = yield* sql`SELECT 1 FROM main.sqlite_schema
      WHERE type = 'table' AND name = 'agent_control_run_once_diagnostics'`;
    if (available.length === 0) return;
    const step = `verification:${implementationResultEvidenceId}`;
    // Select ownership in the write itself: a concurrent completion must not
    // leave a late diagnostic attached to a run that already ended.
    const changed =
      failure === null
        ? yield* sql<{
            readonly projectId: string;
          }>`DELETE FROM main.agent_control_run_once_diagnostics
            WHERE step = ${step} AND EXISTS (
              SELECT 1 FROM main.agent_control_run_once_states run
              JOIN main.agent_control_implementation_result_evidence result
                ON result.project_id = run.project_id AND result.task_id = run.task_id
                  AND result.worktree_reservation_id = run.worktree_reservation_id
              WHERE result.result_evidence_id = ${implementationResultEvidenceId}
                AND run.run_id = agent_control_run_once_diagnostics.run_id
                AND run.project_id = agent_control_run_once_diagnostics.project_id
            )
            RETURNING project_id AS "projectId"`
        : yield* sql<{
            readonly projectId: string;
          }>`INSERT INTO main.agent_control_run_once_diagnostics
          (project_id, run_id, step, error_code, updated_at)
          SELECT run.project_id, run.run_id, ${step}, ${`${failure.operation}: ${failure.reason}`},
            ${DateTime.formatIso(yield* DateTime.now)}
          FROM main.agent_control_run_once_states run
          JOIN main.agent_control_implementation_result_evidence result
            ON result.project_id = run.project_id AND result.task_id = run.task_id
              AND result.worktree_reservation_id = run.worktree_reservation_id
          WHERE result.result_evidence_id = ${implementationResultEvidenceId} AND run.status = 'active'
          ON CONFLICT(project_id) DO UPDATE SET run_id = excluded.run_id, step = excluded.step,
            error_code = excluded.error_code, updated_at = excluded.updated_at
          RETURNING project_id AS "projectId"`;
    for (const row of changed) {
      const notifications = yield* AgentControlRunOnceReadNotifications;
      yield* notifications.publishProject(row.projectId);
    }
  },
  Effect.catch(() => Effect.logWarning("Could not persist verification transition diagnostic")),
);
