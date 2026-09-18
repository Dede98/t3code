import type { ProjectId, AgentControlTaskId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import { loadTaskEpic, saveEpicRun } from "../epic/authority.ts";
import type { AgentControlRunOnceError } from "./model.ts";
import { AgentControlRunOnceReadNotifications } from "./readNotifications.ts";

const hasFailureReason = Schema.is(
  Schema.Struct({ reason: Schema.String, cause: Schema.optional(Schema.Unknown) }),
);
const hasFailureCause = Schema.is(Schema.Struct({ cause: Schema.Unknown }));
const transientReasons = new Set([
  "persistence",
  "mode-inactive",
  "internal-persistence-error",
  "revision-conflict",
  "source-snapshot-unavailable",
  "watermark-missing",
  "watermark-not-completed",
  "watermark-sequence-mismatch",
  "task-sequence-mismatch",
]);
const hasTransitionFailureReason = (failure: unknown, reasons: ReadonlySet<string>): boolean => {
  let current = failure;
  while (hasFailureReason(current) || hasFailureCause(current)) {
    if (hasFailureReason(current) && reasons.has(current.reason)) return true;
    current = current.cause;
  }
  return false;
};

const hasErrorCode = Schema.is(Schema.Struct({ code: Schema.String }));
const permanentPreparationCodes = new Set([
  "authority-conflict",
  "accepted-authority-conflict",
  "command-identity-mismatch",
  "command-previously-rejected",
  "task-projection-corrupt",
  "stage-run-missing",
  "stage-run-not-prepared",
  "stage-run-projection-corrupt",
  "stage-run-history-ambiguous",
  "lease-missing",
  "lease-not-reserved",
  "lease-expired",
  "lease-foreign-runtime",
  "lease-recovery-required",
  "lease-projection-corrupt",
  "fence-token-mismatch",
  "reservation-conflict",
  "reservation-projection-corrupt",
  "repository-identity-mismatch",
  "default-remote-ref-unavailable",
  "worktree-path-invalid",
  "branch-name-invalid",
  "worktree-projection-corrupt",
  "worktree-history-ambiguous",
  "controlled-thread-reservation-identity-conflict",
  "controlled-thread-reservation-corrupt",
]);

/** A retry cannot repair these recorded authority/ownership failures. */
export const epicPreparationBlockerCode = (failure: unknown): string | null =>
  !hasTransitionFailureReason(failure, transientReasons) &&
  hasErrorCode(failure) &&
  permanentPreparationCodes.has(failure.code)
    ? failure.code
    : null;

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
    failure: {
      readonly operation: string;
      readonly reason: string;
      readonly cause?: unknown;
    } | null,
  ) {
    const available = yield* sql`SELECT 1 FROM main.sqlite_schema
      WHERE type = 'table' AND name = 'agent_control_run_once_diagnostics'`;
    if (available.length === 0) return;
    const epicOwners = yield* sql<{
      projectId: ProjectId;
      taskId: AgentControlTaskId;
      worktreeReservationId: string;
    }>`SELECT project_id AS "projectId",task_id AS "taskId",worktree_reservation_id AS "worktreeReservationId"
      FROM agent_control_implementation_result_evidence WHERE result_evidence_id=${implementationResultEvidenceId}`;
    for (const owner of epicOwners)
      yield* persistEpicTransitionDiagnostic(
        sql,
        {
          ...owner,
          transitionId: `verification:${implementationResultEvidenceId}`,
        },
        failure,
      );
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

/** Keep a transition failure on its own Epic member; the project-level Run Once
 * diagnostic row cannot represent parallel execution owners. */
export const persistEpicTransitionDiagnostic = Effect.fn("persistEpicTransitionDiagnostic")(
  function* (
    sql: SqlClient.SqlClient,
    input: {
      readonly projectId: ProjectId;
      readonly taskId: AgentControlTaskId;
      readonly worktreeReservationId: string;
      readonly transitionId: string;
    },
    failure: {
      readonly operation: string;
      readonly reason: string;
      readonly cause?: unknown;
    } | null,
  ) {
    if (hasTransitionFailureReason(failure, transientReasons)) return;
    const changed = yield* sql.withTransaction(
      Effect.gen(function* () {
        const epic = yield* loadTaskEpic(sql, input.projectId, input.taskId);
        if (!epic?.dependencyPlan || (epic.status !== "running" && epic.status !== "blocked"))
          return false;
        if (failure !== null) {
          if (
            epic.status === "blocked" &&
            hasTransitionFailureReason(failure, new Set(["task-status-inactive"]))
          )
            return false;
          const enabled = yield* sql`SELECT 1 FROM agent_control_project_states
            WHERE project_id=${input.projectId} AND mode='armed' AND paused_from_mode IS NULL`;
          if (enabled.length !== 1) return false;
        }
        const member = epic.members.find((item) => item.taskId === input.taskId);
        if (!member || member.status !== "running" || member.childRunId === null) return false;
        const bindings = yield* sql`SELECT 1 FROM agent_control_epic_task_executions
          WHERE execution_id=${member.childRunId} AND epic_run_id=${epic.epicRunId}
            AND project_id=${input.projectId} AND task_id=${input.taskId}
            AND plan_digest=${epic.dependencyPlanDigest!}
            AND worktree_reservation_id=${input.worktreeReservationId}`;
        if (bindings.length !== 1) return false;
        const code = `transition:${input.transitionId}`;
        const previous = [
          ...epic.blockers,
          ...epic.blockerHistory.toReversed().flatMap((entry) => entry.blockers),
        ].find((item) => item.code === code && item.issueNumber === member.issueNumber);
        if (failure === null && (!previous || member.blocker !== previous.message)) return false;
        const detail =
          failure !== null && hasErrorCode(failure.cause) ? `: ${failure.cause.code}` : "";
        const message =
          failure === null
            ? null
            : `Task #${member.issueNumber} could not continue (${failure.operation}: ${failure.reason}${detail}). Inspect its thread and source approval, then resume the Epic after resolving the cause, or stop it. Its execution and worktree are retained.`;
        if (
          epic.status === "blocked" &&
          previous?.message === message &&
          member.blocker === message
        )
          return false;
        const blockers = epic.blockers.filter(
          (item) => item.code !== code || item.issueNumber !== member.issueNumber,
        );
        if (message !== null) blockers.push({ code, issueNumber: member.issueNumber, message });
        yield* saveEpicRun(sql, epic, {
          ...(failure === null ? {} : { status: "blocked" as const }),
          blockers,
          ...(failure === null
            ? {}
            : {
                blockerHistory: [
                  ...epic.blockerHistory,
                  {
                    recordedAt: DateTime.formatIso(yield* DateTime.now),
                    blockers,
                  },
                ],
              }),
          members: epic.members.map((item) => {
            if (item !== member) return item;
            if (message !== null)
              return { ...item, waitReason: "blocker" as const, blocker: message };
            if (item.blocker !== previous?.message) return item;
            const { blocker: _blocker, waitReason: _waitReason, ...retained } = item;
            return retained;
          }),
        });
        return true;
      }),
    );
    if (changed) {
      const notifications = yield* AgentControlRunOnceReadNotifications;
      yield* notifications.publishProject(input.projectId);
    }
  },
);

export const persistImplementationEpicDiagnostic = Effect.fn("persistImplementationEpicDiagnostic")(
  function* (
    sql: SqlClient.SqlClient,
    handoffId: string,
    failure: {
      readonly operation: string;
      readonly reason: string;
      readonly cause?: unknown;
    } | null,
  ) {
    const rows = yield* sql<{
      projectId: ProjectId;
      taskId: AgentControlTaskId;
      worktreeReservationId: string;
    }>`SELECT project_id AS "projectId", task_id AS "taskId",
      worktree_reservation_id AS "worktreeReservationId"
      FROM agent_control_initial_planning_handoff_intents WHERE handoff_id=${handoffId}`;
    if (!rows[0]) return;
    yield* persistEpicTransitionDiagnostic(
      sql,
      {
        ...rows[0],
        transitionId: `implementation:${handoffId}`,
      },
      failure,
    );
  },
  Effect.catch(() =>
    Effect.logWarning("Could not persist Epic implementation transition diagnostic"),
  ),
);
