import { AgentControlTaskId, type ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

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

export const selectAgentControlRunOnceCandidate = Effect.fn("selectAgentControlRunOnceCandidate")(
  function* (sql: SqlClient.SqlClient, projectId: ProjectId, githubIntakeSequence: number) {
    const rows = yield* sql.unsafe<Record<string, unknown>>(AGENT_CONTROL_RUN_ONCE_CANDIDATE_SQL, [
      projectId,
      githubIntakeSequence,
    ]).unprepared;
    const decoded = yield* decodeRows(rows);
    if (decoded.length > 1) return yield* Effect.die(new Error("run-once LIMIT invariant"));
    return decoded[0]?.taskId ?? null;
  },
);
