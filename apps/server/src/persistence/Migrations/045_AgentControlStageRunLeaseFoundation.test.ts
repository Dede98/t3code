import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const at = "2026-07-24T10:00:00.000Z";

layer("045_AgentControlStageRunLeaseFoundation", (it) => {
  it.effect("preserves existing CQRS data and adds isolated lease constraints", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 44 });

      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-045-stage', 'stage-run', 'stage-run-045', 1,
          'agentControl.stageRun.prepared', ${at}, 'command-045-stage',
          NULL, 'command-045-stage', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-045-stage', 'fingerprint-045', 'controller', 'stage-run',
          'stage-run-045', 'accepted', 1, 1, 1, ${at}, NULL
        )
      `;
      yield* sql`
        INSERT INTO agent_control_stage_run_states (
          stage_run_id, project_id, task_id, attempt_id, role_id,
          stage_kind, stage_ordinal, attempt_ordinal, status,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          state_json, created_at, updated_at, revision, last_event_sequence
        ) VALUES (
          'stage-run-045', 'project-045', 'task-045', 'attempt-045', 'planning',
          'planning', 1, 1, 'prepared', 1, 1,
          'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          '{}', ${at}, ${at}, 1, 1
        )
      `;
      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          scripts_json, created_at, updated_at, deleted_at
        ) VALUES (
          'project-045', 'Preserved project', '/tmp/project-045', NULL, '[]',
          ${at}, ${at}, NULL
        )
      `;
      yield* sql`
        INSERT INTO agent_control_project_states (
          project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
        ) VALUES ('project-045', 'observe', NULL, 1, 1, ${at})
      `;
      yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id, policy_json, revision, updated_at
        ) VALUES ('project-045', '{}', 1, ${at})
      `;
      yield* sql`
        INSERT INTO agent_control_github_intake_states (
          project_id, state_json, revision, last_event_sequence, updated_at
        ) VALUES ('project-045', '{}', 1, 1, ${at})
      `;
      yield* sql`
        INSERT INTO agent_control_github_scheduler_states (
          project_id, state_json, generation, last_github_event_sequence,
          activity, circuit_state, consecutive_failures, last_attempt_at,
          next_attempt_at, cooldown_until, reason_code, updated_at, scheduler_revision
        ) VALUES (
          'project-045', '{}', 1, 1, 'active', 'closed', 0, NULL, NULL,
          NULL, NULL, ${at}, 1
        )
      `;
      yield* sql`
        INSERT INTO agent_control_task_states (
          task_id, project_id, repository_node_id, issue_node_id,
          issue_number, issue_url, status, source_gate, stage,
          source_updated_at, github_intake_sequence, state_json,
          created_at, updated_at, revision, last_event_sequence
        ) VALUES (
          'task-045', 'project-045', 'repo-045', 'issue-045', 45,
          'https://example.test/issues/45', 'candidate', 'eligible', 'intake',
          ${at}, 1, '{}', ${at}, ${at}, 1, 1
        )
      `;
      yield* sql`
        INSERT INTO agent_control_task_reconcile_states (
          project_id, target_sequence, last_completed_sequence,
          revision, status, updated_at
        ) VALUES ('project-045', 1, 1, 1, 'completed', ${at})
      `;
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'orchestration-event-045', 'project', 'project-045', 1,
          'project.created', ${at}, 'orchestration-command-045', NULL,
          'orchestration-command-045', 'client', '{}', '{}'
        )
      `;
      const preservedTables = [
        "agent_control_events",
        "agent_control_command_receipts",
        "agent_control_project_states",
        "agent_control_project_policies",
        "agent_control_github_intake_states",
        "agent_control_github_scheduler_states",
        "agent_control_task_states",
        "agent_control_task_reconcile_states",
        "agent_control_stage_run_states",
        "orchestration_events",
        "projection_projects",
      ] as const;
      const before = new Map<string, number>();
      for (const table of preservedTables) {
        before.set(
          table,
          (yield* sql.unsafe<{ readonly count: number }>(
            `SELECT COUNT(*) AS count FROM ${table}`,
          ))[0]!.count,
        );
      }

      yield* runMigrations({ toMigrationInclusive: 45 });

      for (const table of preservedTables) {
        assert.equal(
          (yield* sql.unsafe<{ readonly count: number }>(
            `SELECT COUNT(*) AS count FROM ${table}`,
          ))[0]!.count,
          before.get(table),
        );
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list('agent_control_stage_run_lease_states')
        `)[0]!.count,
        0,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM pragma_index_info('idx_agent_control_stage_run_lease_scope')
          ORDER BY seqno
        `,
        [{ name: "project_id" }, { name: "task_id" }],
      );

      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-045-lease', 'stage-run-lease', 'lease-045', 1,
          'agentControl.stageRunLease.reserved', ${at}, 'command-045-lease',
          NULL, 'command-045-lease', 'system', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-045-rejected', 'fingerprint-lease', 'controller',
          'stage-run-lease', 'lease-045', 'rejected', 1, 1, 0, ${at},
          'lease-already-reserved'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_stage_run_lease_states (
          lease_id, project_id, task_id, stage_run_id, attempt_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          holder_id, fence_token, status, acquired_at, renewed_at,
          expires_at, released_at, state_json, revision, last_event_sequence
        ) VALUES (
          'lease-045', 'project-045', 'task-045', 'stage-run-045', 'attempt-045',
          1, 1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          'holder-045', 1, 'reserved', ${at}, ${at},
          '2026-07-24T10:01:00.000Z', NULL, '{}', 1, 2
        )
      `;
      const duplicateScope = yield* Effect.result(sql`
        INSERT INTO agent_control_stage_run_lease_states (
          lease_id, project_id, task_id, stage_run_id, attempt_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          holder_id, fence_token, status, acquired_at, renewed_at,
          expires_at, released_at, state_json, revision, last_event_sequence
        ) VALUES (
          'lease-045-other', 'project-045', 'task-045', 'stage-run-other',
          'attempt-other', 1, 1,
          'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
          'holder-other', 2, 'reserved', ${at}, ${at},
          '2026-07-24T10:01:00.000Z', NULL, '{}', 1, 3
        )
      `);
      assert.equal(duplicateScope._tag, "Failure");
    }),
  );
});
