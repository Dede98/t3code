import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("042_AgentControlTaskIntakeCqrs", (it) => {
  it.effect(
    "widens task constraints without replacing prior Agent Control or orchestration data",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 41 });

        yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'orch-preserved', 'project', 'project-preserved', 1, 'project.created',
          '2026-07-23T10:00:00.000Z', 'orch-command', NULL, 'orch-command',
          'client', '{}', '{}'
        )
      `;
        yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          scripts_json, created_at, updated_at, deleted_at
        ) VALUES (
          'project-preserved', 'Preserved', '/tmp/preserved', NULL, '[]',
          '2026-07-23T10:00:00.000Z', '2026-07-23T10:00:00.000Z', NULL
        )
      `;
        yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'controller-preserved', 'project-controller', 'project-preserved', 1,
          'agentControl.project.mode.changed', '2026-07-23T10:00:00.000Z',
          'controller-command', NULL, 'controller-command', 'human', '{}',
          '{"schemaVersion":1}'
        ), (
          'github-preserved', 'github-intake', 'project-preserved', 1,
          'agentControl.github.config.set', '2026-07-23T10:00:01.000Z',
          'github-command', NULL, 'github-command', 'human', '{}',
          '{"schemaVersion":1}'
        )
      `;
        yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'controller-command', 'controller-fingerprint', 'human',
          'project-controller', 'project-preserved', 'accepted', 1, 1, 1,
          '2026-07-23T10:00:00.000Z', NULL
        ), (
          'github-command', 'github-fingerprint', 'human',
          'github-intake', 'project-preserved', 'accepted', 2, 1, 1,
          '2026-07-23T10:00:01.000Z', NULL
        )
      `;
        yield* sql`
        INSERT INTO agent_control_project_states (
          project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
        ) VALUES (
          'project-preserved', 'observe', NULL, 1, 1, '2026-07-23T10:00:00.000Z'
        )
      `;
        yield* sql`
        INSERT INTO agent_control_github_intake_states (
          project_id, state_json, revision, last_event_sequence, updated_at
        ) VALUES (
          'project-preserved', '{}', 1, 2, '2026-07-23T10:00:01.000Z'
        )
      `;
        yield* sql`
        INSERT INTO agent_control_github_issues (
          project_id, issue_node_id, issue_number, repository_node_id,
          snapshot_json, updated_at
        ) VALUES (
          'project-preserved', 'issue-preserved', 1, 'repo-preserved', '{}',
          '2026-07-23T10:00:01.000Z'
        )
      `;
        yield* sql`
        INSERT INTO agent_control_github_scheduler_states (
          project_id, state_json, generation, last_github_event_sequence,
          activity, circuit_state, consecutive_failures, last_attempt_at,
          next_attempt_at, cooldown_until, reason_code, updated_at, scheduler_revision
        ) VALUES (
          'project-preserved', '{}', 1, 2, 'active', 'closed', 0, NULL, NULL,
          NULL, NULL, '2026-07-23T10:00:01.000Z', 1
        )
      `;
        yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id, policy_json, revision, updated_at
        ) VALUES (
          'project-preserved', '{"fullAccess":false}', 1,
          '2026-07-23T10:00:00.000Z'
        )
      `;

        yield* runMigrations({ toMigrationInclusive: 42 });

        assert.deepStrictEqual(
          yield* sql`
          SELECT event_id, aggregate_kind, stream_id, stream_version
          FROM agent_control_events ORDER BY sequence
        `,
          [
            {
              event_id: "controller-preserved",
              aggregate_kind: "project-controller",
              stream_id: "project-preserved",
              stream_version: 1,
            },
            {
              event_id: "github-preserved",
              aggregate_kind: "github-intake",
              stream_id: "project-preserved",
              stream_version: 1,
            },
          ],
        );
        assert.deepStrictEqual(
          yield* sql`
          SELECT command_id, aggregate_kind, status
          FROM agent_control_command_receipts ORDER BY command_id
        `,
          [
            {
              command_id: "controller-command",
              aggregate_kind: "project-controller",
              status: "accepted",
            },
            {
              command_id: "github-command",
              aggregate_kind: "github-intake",
              status: "accepted",
            },
          ],
        );
        for (const table of [
          "orchestration_events",
          "projection_projects",
          "agent_control_project_states",
          "agent_control_github_intake_states",
          "agent_control_github_issues",
          "agent_control_github_scheduler_states",
          "agent_control_project_policies",
        ]) {
          const rows = yield* sql.unsafe<{ readonly count: number }>(
            `SELECT COUNT(*) AS count FROM ${table}`,
          );
          assert.equal(rows[0]?.count, 1);
        }

        yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'task-new', 'task', 'task-new', 1, 'agentControl.task.created',
          '2026-07-23T11:00:00.000Z', 'task-command', NULL, 'task-command',
          'controller', '{}', '{"schemaVersion":1}'
        )
      `;
        yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'task-rejected', 'task-fingerprint', 'controller', 'task', 'task-new',
          'rejected', 0, 0, 0, '2026-07-23T11:00:00.000Z',
          'state-not-available'
        )
      `;
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_task_states
          `)[0]?.count,
          0,
        );
      }),
  );
});
