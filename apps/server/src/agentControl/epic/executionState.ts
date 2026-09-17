import type { ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

/** Disarm lets the current turn settle but admits no next stage. Pending, unentered
 * requests are safe to leave; uncertain deliveries and held capacity are not. */
export const unsettledEpicExecutions = Effect.fn("unsettledEpicExecutions")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
) {
  const installed = yield* sql`SELECT 1 FROM sqlite_schema
    WHERE name='agent_control_epic_task_executions' AND type='table'`;
  if (installed.length === 0) return new Set<string>();
  const rows = yield* sql<{ executionId: string }>`
    SELECT execution.execution_id AS "executionId"
    FROM agent_control_epic_task_executions execution
    LEFT JOIN agent_control_project_states project ON project.project_id=execution.project_id
    LEFT JOIN agent_control_task_states task
      ON task.project_id=execution.project_id AND task.task_id=execution.task_id
    WHERE execution.project_id=${projectId} AND (
      EXISTS (
        SELECT 1 FROM resource_admission_provider_requests request
        WHERE (request.status IN ('admitted','entered') OR (request.status='waiting' AND project.mode='armed')) AND request.handoff_id IN (
          SELECT handoff_id FROM agent_control_initial_planning_handoff_intents
            WHERE project_id=execution.project_id AND task_id=execution.task_id
          UNION ALL SELECT handoff_id FROM agent_control_implementation_handoff_intents
            WHERE project_id=execution.project_id AND task_id=execution.task_id
          UNION ALL SELECT handoff_id FROM agent_control_verification_handoff_intents
            WHERE project_id=execution.project_id AND task_id=execution.task_id
        )
      ) OR EXISTS (
        SELECT 1 FROM agent_control_controlled_thread_reservation_states thread
        WHERE thread.project_id=execution.project_id AND thread.task_id=execution.task_id
          AND thread.status='materializing'
      ) OR EXISTS (
        SELECT 1 FROM (
          SELECT intent.project_id,intent.task_id,delivery.state
            FROM agent_control_initial_planning_handoff_intents intent
            JOIN agent_control_initial_planning_deliveries delivery ON delivery.handoff_id=intent.handoff_id
          UNION ALL SELECT intent.project_id,intent.task_id,delivery.state
            FROM agent_control_implementation_handoff_intents intent
            JOIN agent_control_implementation_deliveries delivery ON delivery.handoff_id=intent.handoff_id
          UNION ALL SELECT intent.project_id,intent.task_id,delivery.state
            FROM agent_control_verification_handoff_intents intent
            JOIN agent_control_verification_deliveries delivery ON delivery.handoff_id=intent.handoff_id
        ) delivery WHERE delivery.project_id=execution.project_id AND delivery.task_id=execution.task_id
          AND delivery.state IN ('claimed','delivery-attempted','provider-started','interrupt-requested','ambiguous')
      ) OR (
        project.mode='armed' AND
        (execution.thread_id IS NOT NULL OR EXISTS (
          SELECT 1 FROM agent_control_controlled_thread_reservation_states thread
          WHERE thread.project_id=execution.project_id AND thread.task_id=execution.task_id
            AND thread.status IN ('materializing','bound')
        ))
        AND COALESCE(task.status,'') NOT IN ('failed','cancelled','needs-attention')
        AND NOT EXISTS (
          SELECT 1 FROM agent_control_task_verification_finalization_evidence evidence
          JOIN agent_control_task_verification_finalization_receipts receipt
            ON receipt.task_finalization_evidence_id=evidence.task_finalization_evidence_id
              AND receipt.status='accepted'
          JOIN agent_control_task_verification_finalization_markers marker
            ON marker.task_finalization_evidence_id=evidence.task_finalization_evidence_id
              AND marker.receipt_id=receipt.receipt_id
          WHERE evidence.project_id=execution.project_id AND evidence.task_id=execution.task_id
        )
      )
    )`;
  return new Set(rows.map((row) => row.executionId));
});
