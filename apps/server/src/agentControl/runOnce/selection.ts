import { loadEnabledEpicQueue } from "../epic/queueAuthority.ts";
import {
  AgentControlTaskId,
  type AgentControlEpicRuntimeView,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import { epicError, loadProjectEpics } from "../epic/authority.ts";

export const AGENT_CONTROL_RUN_ONCE_CANDIDATE_SQL = `SELECT candidate.task_id AS "taskId"
FROM main.agent_control_task_states AS candidate
INDEXED BY idx_agent_control_run_once_candidates
WHERE candidate.project_id = ?
  AND candidate.github_intake_sequence = ?
  AND candidate.status = 'candidate'
  AND candidate.source_gate = 'eligible'
  AND candidate.stage = 'intake'
ORDER BY candidate.issue_number ASC, candidate.task_id ASC
LIMIT 1`;

const CandidateRow = Schema.Struct({ taskId: AgentControlTaskId });
const decodeRows = Schema.decodeUnknownEffect(Schema.Array(CandidateRow));

export const isEpicChildRunOnceOwned = Effect.fn("isEpicChildRunOnceOwned")(function* (
  sql: SqlClient.SqlClient,
  epic: AgentControlEpicRuntimeView,
) {
  if (epic.dependencyPlan || epic.status !== "running" || epic.activeTaskId === null) return false;
  const members = epic.members.filter((member) => member.taskId === epic.activeTaskId);
  const member = members[0];
  if (members.length !== 1 || member?.status !== "running")
    return yield* epicError("authority-conflict", "The active Epic child is ambiguous.");
  if (member.childRunId !== null) {
    // Observe can finish the Run Once controller while its admitted turn
    // still settles. Re-arming must wait for Epic progress to accept that
    // child's result, rather than admit its unchanged intake task again.
    const owned = yield* sql`
    SELECT run.run_id
    FROM main.agent_control_run_once_states run
    JOIN main.agent_control_run_once_activations activation
      ON activation.run_id=run.run_id AND activation.project_id=run.project_id
    JOIN main.agent_control_run_once_step_evidence selected
      ON selected.run_id=run.run_id AND selected.project_id=run.project_id
        AND selected.step='task-selected' AND selected.task_id=run.task_id
    JOIN main.agent_control_run_once_step_receipts selected_receipt
      ON selected_receipt.evidence_id=selected.evidence_id
        AND selected_receipt.receipt_id=selected.receipt_id
        AND selected_receipt.status='accepted'
    JOIN main.agent_control_run_once_step_markers selected_marker
      ON selected_marker.evidence_id=selected.evidence_id
        AND selected_marker.receipt_id=selected_receipt.receipt_id
        AND selected_marker.marker_id=selected.marker_id
    JOIN main.agent_control_run_once_step_evidence latest
      ON latest.run_id=run.run_id AND latest.project_id=run.project_id
        AND latest.ordinal=run.next_ordinal-1 AND latest.step=run.last_step
        AND latest.task_id=run.task_id
        AND latest.stage_run_id IS run.stage_run_id
        AND latest.lease_id IS run.lease_id
        AND latest.worktree_reservation_id IS run.worktree_reservation_id
        AND latest.controlled_thread_reservation_id IS run.controlled_thread_reservation_id
        AND latest.terminal_task_event_id IS run.terminal_task_event_id
    JOIN main.agent_control_run_once_step_receipts receipt
      ON receipt.evidence_id=latest.evidence_id AND receipt.receipt_id=latest.receipt_id
        AND receipt.status='accepted'
    JOIN main.agent_control_run_once_step_markers marker
      ON marker.evidence_id=latest.evidence_id AND marker.receipt_id=receipt.receipt_id
        AND marker.marker_id=latest.marker_id
    WHERE run.run_id=${member.childRunId} AND run.project_id=${epic.projectId}
      AND run.task_id=${epic.activeTaskId} AND run.status IN ('active','completed')
    `;
    if (owned.length !== 1)
      return yield* epicError(
        "authority-conflict",
        "The active Epic child has no matching Run Once authority.",
      );
    return true;
  }
  return false;
});

export const selectAgentControlRunOnceCandidate = Effect.fn("selectAgentControlRunOnceCandidate")(
  function* (sql: SqlClient.SqlClient, projectId: ProjectId, githubIntakeSequence: number) {
    const epics = yield* loadProjectEpics(sql, projectId);
    if (epics.length > 1 || epics.some((epic) => epic.dependencyPlan)) return null;
    const epic = epics[0];
    if (epic !== undefined) {
      if (epic.status !== "running" || epic.activeTaskId === null) return null;
      if (yield* isEpicChildRunOnceOwned(sql, epic)) return null;
      const rows = yield* sql<
        Record<string, unknown>
      >`SELECT task_id AS "taskId" FROM main.agent_control_task_states
        WHERE project_id=${projectId} AND task_id=${epic.activeTaskId}
        AND github_intake_sequence=${githubIntakeSequence} AND status='candidate' AND source_gate='eligible' AND stage='intake'`;
      return (yield* decodeRows(rows))[0]?.taskId ?? null;
    }
    if (yield* loadEnabledEpicQueue(sql, projectId)) return null;
    const rows = yield* sql.unsafe<Record<string, unknown>>(AGENT_CONTROL_RUN_ONCE_CANDIDATE_SQL, [
      projectId,
      githubIntakeSequence,
    ]).unprepared;
    const decoded = yield* decodeRows(rows);
    if (decoded.length > 1) return yield* Effect.die(new Error("run-once LIMIT invariant"));
    return decoded[0]?.taskId ?? null;
  },
);

/**
 * The deterministic first candidate is never skipped. Any pre-existing Initial
 * execution authority makes that selected candidate divergent and the caller
 * must fail closed.
 */
export const isAgentControlRunOnceCandidateVacant = Effect.fn(
  "isAgentControlRunOnceCandidateVacant",
)(function* (sql: SqlClient.SqlClient, projectId: ProjectId, taskId: AgentControlTaskId) {
  const rows = yield* sql<{ readonly count: unknown }>`
    SELECT (
      (SELECT COUNT(*) FROM main.agent_control_events event
       WHERE event.aggregate_kind IN (
         'stage-run', 'stage-run-lease', 'worktree-reservation',
         'controlled-thread-reservation'
       )
       AND json_extract(event.payload_json, '$.projectId') = ${projectId}
       AND json_extract(event.payload_json, '$.taskId') = ${taskId})
      + (SELECT COUNT(*) FROM main.agent_control_stage_run_states
         WHERE project_id = ${projectId} AND task_id = ${taskId})
      + (SELECT COUNT(*) FROM main.agent_control_stage_run_lease_states
         WHERE project_id = ${projectId} AND task_id = ${taskId})
      + (SELECT COUNT(*) FROM main.agent_control_worktree_reservation_states
         WHERE project_id = ${projectId} AND task_id = ${taskId})
      + (SELECT COUNT(*) FROM main.agent_control_controlled_thread_reservation_states
         WHERE project_id = ${projectId} AND task_id = ${taskId})
      + (SELECT COUNT(*) FROM main.agent_control_controlled_thread_materialization_intents
         WHERE project_id = ${projectId} AND task_id = ${taskId})
    ) AS count
  `;
  return rows.length === 1 && rows[0]?.count === 0;
});
