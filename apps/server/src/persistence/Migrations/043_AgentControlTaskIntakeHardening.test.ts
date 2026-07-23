import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const now = "2026-07-23T10:00:00.000Z";

const insertTaskState = (
  sql: SqlClient.SqlClient,
  input: {
    readonly taskId: string;
    readonly issueNodeId: string;
    readonly issueNumber: number;
  },
) =>
  sql`
    INSERT INTO agent_control_task_states (
      task_id, project_id, repository_node_id, issue_node_id,
      issue_number, issue_url, status, source_gate, stage,
      source_updated_at, github_intake_sequence, state_json,
      created_at, updated_at, revision, last_event_sequence
    ) VALUES (
      ${input.taskId}, 'project-043', 'repo-043', ${input.issueNodeId},
      ${input.issueNumber}, ${`https://github.test/o/r/issues/${input.issueNumber}`},
      'candidate', 'eligible', 'intake', ${now}, 1, '{}',
      ${now}, ${now}, 1, 1
    )
  `;

layer("043_AgentControlTaskIntakeHardening", (it) => {
  it.effect("adds recovery constraints, source-number uniqueness, and reconcile state", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 42 });
      yield* insertTaskState(sql, {
        taskId: "task-043-one",
        issueNodeId: "issue-043-one",
        issueNumber: 1,
      });

      yield* runMigrations({ toMigrationInclusive: 43 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-043-recovered', 'task', 'task-043-one', 1,
          'agentControl.task.sourceMissingRecovered', ${now}, 'command-043-recovered',
          NULL, 'command-043-recovered', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-043-stale', 'fingerprint', 'controller', 'task', 'task-043-one',
          'rejected', 0, 0, 0, ${now}, 'source-snapshot-stale'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_task_reconcile_states (
          project_id, target_sequence, last_completed_sequence,
          revision, status, updated_at
        ) VALUES ('project-043', 2, 1, 1, 'recovery-required', ${now})
      `;

      const duplicate = yield* Effect.result(
        insertTaskState(sql, {
          taskId: "task-043-two",
          issueNodeId: "issue-043-two",
          issueNumber: 1,
        }),
      );
      assert.equal(duplicate._tag, "Failure");
      assert.deepStrictEqual(
        yield* sql`
          SELECT target_sequence, last_completed_sequence, status
          FROM agent_control_task_reconcile_states
          WHERE project_id = 'project-043'
        `,
        [
          {
            target_sequence: 2,
            last_completed_sequence: 1,
            status: "recovery-required",
          },
        ],
      );
    }),
  );
});

const duplicateLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

duplicateLayer("043_AgentControlTaskIntakeHardening duplicate guard", (it) => {
  it.effect("fails migration instead of choosing among duplicate source numbers", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 42 });
      yield* insertTaskState(sql, {
        taskId: "task-043-duplicate-one",
        issueNodeId: "issue-043-duplicate-one",
        issueNumber: 7,
      });
      yield* insertTaskState(sql, {
        taskId: "task-043-duplicate-two",
        issueNodeId: "issue-043-duplicate-two",
        issueNumber: 7,
      });

      const migrated = yield* Effect.exit(runMigrations({ toMigrationInclusive: 43 }));
      assert.equal(migrated._tag, "Failure");
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_task_states
          WHERE project_id = 'project-043'
        `)[0]?.count,
        2,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM sqlite_master
          WHERE type = 'table' AND name = 'agent_control_task_reconcile_states'
        `)[0]?.count,
        0,
      );
    }),
  );
});
