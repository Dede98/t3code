import { ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import {
  AGENT_CONTROL_RUN_ONCE_CANDIDATE_SQL,
  isAgentControlRunOnceCandidateVacant,
  selectAgentControlRunOnceCandidate,
} from "./selection.ts";

const layer = it.layer(NodeSqliteClient.layerMemory());

layer("run-once candidate selection", (it) => {
  it.effect("uses the exact covering index and orders issue number then task id", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 63 });
      const projectId = ProjectId.make("run-once-selection");
      const insert = (input: {
        readonly taskId: string;
        readonly issueNumber: number;
        readonly status?: string;
        readonly sourceGate?: string;
        readonly stage?: string;
        readonly sequence?: number;
      }) =>
        sql`
          INSERT INTO main.agent_control_task_states (
            task_id, project_id, repository_node_id, issue_node_id, issue_number,
            issue_url, status, source_gate, stage, source_updated_at,
            github_intake_sequence, state_json, created_at, updated_at,
            revision, last_event_sequence
          ) VALUES (
            ${input.taskId}, ${projectId}, ${`repository-${input.taskId}`}, ${`issue-${input.taskId}`},
            ${input.issueNumber}, ${`https://example.invalid/${input.issueNumber}`},
            ${input.status ?? "candidate"}, ${input.sourceGate ?? "eligible"},
            ${input.stage ?? "intake"}, '2026-08-31T10:00:00.000Z',
            ${input.sequence ?? 7}, '{}', '2026-08-31T10:00:00.000Z',
            '2026-08-31T10:00:00.000Z', 1, 1
          )
        `;
      yield* insert({ taskId: "task-z", issueNumber: 2 });
      yield* insert({ taskId: "task-b", issueNumber: 1 });
      yield* insert({ taskId: "task-a", issueNumber: 1 });
      yield* insert({ taskId: "wrong-status", issueNumber: 0 + 1, status: "running" });
      yield* insert({ taskId: "wrong-gate", issueNumber: 1, sourceGate: "paused" });
      yield* insert({ taskId: "wrong-sequence", issueNumber: 1, sequence: 8 });

      const selected = yield* selectAgentControlRunOnceCandidate(sql, projectId, 7);
      assert.equal(selected, "task-a");
      yield* sql`
        INSERT INTO main.agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'manual-stage-event', 'stage-run', 'manual-stage-run', 1,
          'agentControl.stageRun.prepared', '2026-08-31T10:00:00.000Z',
          'manual-stage-command', NULL, 'manual-stage-command', 'controller',
          ${JSON.stringify({ projectId, taskId: "task-a" })}, '{"schemaVersion":1}'
        )
      `;
      assert.isFalse(yield* isAgentControlRunOnceCandidateVacant(sql, projectId, selected!));
      yield* sql`
        INSERT INTO main.agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'ambiguous-stage-event', 'stage-run', 'ambiguous-stage-run', 1,
          'agentControl.stageRun.prepared', '2026-08-31T10:00:00.000Z',
          'ambiguous-stage-command', NULL, 'ambiguous-stage-command', 'controller',
          ${JSON.stringify({ projectId, taskId: "task-a" })}, '{"schemaVersion":1}'
        )
      `;
      assert.equal(yield* selectAgentControlRunOnceCandidate(sql, projectId, 7), "task-a");
      assert.isFalse(yield* isAgentControlRunOnceCandidateVacant(sql, projectId, selected!));
      assert.equal(
        yield* selectAgentControlRunOnceCandidate(sql, ProjectId.make("empty-project"), 7),
        null,
      );
      const plan = yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${AGENT_CONTROL_RUN_ONCE_CANDIDATE_SQL}`,
        [projectId, 7],
      ).unprepared;
      assert.isTrue(
        plan.some((row) =>
          row.detail.includes("COVERING INDEX idx_agent_control_run_once_candidates"),
        ),
      );
      assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")));
    }),
  );
});
