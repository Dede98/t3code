import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const at = "2026-07-24T10:00:00.000Z";

layer("044_AgentControlStageRunCqrs", (it) => {
  it.effect("preserves all existing Agent-Control data and adds isolated stage-run storage", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 43 });

      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-044-controller', 'project-controller', 'project-044', 1,
          'agentControl.project.mode.changed', ${at}, 'command-044-controller',
          NULL, 'command-044-controller', 'human', '{}', '{"schemaVersion":1}'
        ), (
          'event-044-task', 'task', 'task-044', 1,
          'agentControl.task.created', ${at}, 'command-044-task',
          NULL, 'command-044-task', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-044-controller', 'fingerprint-controller', 'human',
          'project-controller', 'project-044', 'accepted', 1, 1, 1, ${at}, NULL
        ), (
          'command-044-task', 'fingerprint-task', 'controller',
          'task', 'task-044', 'accepted', 2, 1, 1, ${at}, NULL
        )
      `;
      yield* sql`
        INSERT INTO agent_control_task_states (
          task_id, project_id, repository_node_id, issue_node_id,
          issue_number, issue_url, status, source_gate, stage,
          source_updated_at, github_intake_sequence, state_json,
          created_at, updated_at, revision, last_event_sequence
        ) VALUES (
          'task-044', 'project-044', 'repo-044', 'issue-044', 44,
          'https://example.test/issues/44', 'candidate', 'eligible', 'intake',
          ${at}, 2, '{}', ${at}, ${at}, 1, 2
        )
      `;
      yield* sql`
        INSERT INTO agent_control_task_reconcile_states (
          project_id, target_sequence, last_completed_sequence,
          revision, status, updated_at
        ) VALUES ('project-044', 2, 2, 1, 'completed', ${at})
      `;
      yield* sql`
        INSERT INTO agent_control_project_states (
          project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
        ) VALUES ('project-044', 'observe', NULL, 1, 1, ${at})
      `;
      yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id, policy_json, revision, updated_at
        ) VALUES ('project-044', '{}', 1, ${at})
      `;
      yield* sql`
        INSERT INTO agent_control_projection_state (
          projector_name, last_applied_sequence, updated_at
        ) VALUES ('unrelated-projector-044', 2, ${at})
      `;
      yield* sql`
        INSERT INTO agent_control_github_intake_states (
          project_id, state_json, revision, last_event_sequence, updated_at
        ) VALUES ('project-044', '{}', 1, 2, ${at})
      `;
      yield* sql`
        INSERT INTO agent_control_github_issues (
          project_id, issue_node_id, issue_number, repository_node_id,
          snapshot_json, updated_at
        ) VALUES ('project-044', 'github-issue-044', 44, 'repo-044', '{}', ${at})
      `;
      yield* sql`
        INSERT INTO agent_control_github_scheduler_states (
          project_id, state_json, generation, last_github_event_sequence,
          activity, circuit_state, consecutive_failures, last_attempt_at,
          next_attempt_at, cooldown_until, reason_code, updated_at, scheduler_revision
        ) VALUES (
          'project-044', '{}', 1, 2, 'active', 'closed', 0, NULL, NULL,
          NULL, NULL, ${at}, 1
        )
      `;
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'orchestration-event-044', 'project', 'project-044', 1,
          'project.created', ${at}, 'orchestration-command-044', NULL,
          'orchestration-command-044', 'client', '{}', '{}'
        )
      `;
      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          scripts_json, created_at, updated_at, deleted_at
        ) VALUES (
          'project-044', 'Preserved project', '/tmp/project-044', NULL, '[]',
          ${at}, ${at}, NULL
        )
      `;

      const existingTables = [
        "agent_control_events",
        "agent_control_command_receipts",
        "agent_control_task_states",
        "agent_control_task_reconcile_states",
        "agent_control_project_states",
        "agent_control_project_policies",
        "agent_control_projection_state",
        "agent_control_github_intake_states",
        "agent_control_github_issues",
        "agent_control_github_scheduler_states",
        "orchestration_events",
        "projection_projects",
      ] as const;
      const before = new Map<string, number>();
      for (const table of existingTables) {
        before.set(
          table,
          (yield* sql.unsafe<{ readonly count: number }>(
            `SELECT COUNT(*) AS count FROM ${table}`,
          ))[0]!.count,
        );
      }

      yield* runMigrations({ toMigrationInclusive: 44 });

      for (const table of existingTables) {
        assert.equal(
          (yield* sql.unsafe<{ readonly count: number }>(
            `SELECT COUNT(*) AS count FROM ${table}`,
          ))[0]!.count,
          before.get(table),
        );
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM pragma_foreign_key_list('agent_control_stage_run_states')
        `)[0]?.count,
        0,
      );

      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-044-stage', 'stage-run', 'stage-run-044', 1,
          'agentControl.stageRun.prepared', ${at}, 'command-044-stage',
          NULL, 'command-044-stage', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-044-stage-rejected', 'fingerprint-stage', 'controller',
          'stage-run', 'stage-run-044', 'rejected', 0, 0, 0, ${at},
          'source-watermark-stale'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_stage_run_states (
          stage_run_id, project_id, task_id, attempt_id, role_id,
          stage_kind, stage_ordinal, attempt_ordinal, status,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          state_json, created_at, updated_at, revision, last_event_sequence
        ) VALUES (
          'stage-run-044', 'project-044', 'task-044', 'attempt-044', 'planning',
          'planning', 1, 1, 'prepared', 1, 2, 'source-fingerprint',
          '{}', ${at}, ${at}, 1, 3
        )
      `;
      const duplicate = yield* Effect.result(sql`
        INSERT INTO agent_control_stage_run_states (
          stage_run_id, project_id, task_id, attempt_id, role_id,
          stage_kind, stage_ordinal, attempt_ordinal, status,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          state_json, created_at, updated_at, revision, last_event_sequence
        ) VALUES (
          'stage-run-044-duplicate', 'project-044', 'task-044',
          'attempt-044-duplicate', 'planning', 'planning', 1, 1, 'prepared',
          1, 2, 'source-fingerprint', '{}', ${at}, ${at}, 1, 3
        )
      `);
      assert.equal(duplicate._tag, "Failure");
    }),
  );
});
