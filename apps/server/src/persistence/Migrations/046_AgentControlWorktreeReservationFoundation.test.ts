import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const at = "2026-07-24T10:00:00.000Z";

layer("046_AgentControlWorktreeReservationFoundation", (it) => {
  it.effect("preserves prior Agent Control data and adds isolated reservation constraints", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 45 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-before-046', 'stage-run-lease', 'lease-before-046', 1,
          'agentControl.stageRunLease.reserved', ${at}, 'command-before-046',
          NULL, 'command-before-046', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      const sequenceBefore = (yield* sql<{ readonly sequence: number }>`
        SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
      `)[0]!.sequence;
      const eventBefore = yield* sql`
        SELECT * FROM agent_control_events WHERE event_id = 'event-before-046'
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-before-046', 'fingerprint-before-046', 'controller',
          'stage-run-lease', 'lease-before-046', 'accepted', 1, 1, 1, ${at}, NULL
        )
      `;
      const receiptBefore = yield* sql`
        SELECT * FROM agent_control_command_receipts
        WHERE command_id = 'command-before-046'
      `;
      const tables = [
        "agent_control_events",
        "agent_control_command_receipts",
        "agent_control_project_states",
        "agent_control_project_policies",
        "agent_control_github_intake_states",
        "agent_control_github_scheduler_states",
        "agent_control_task_states",
        "agent_control_task_reconcile_states",
        "agent_control_stage_run_states",
        "agent_control_stage_run_lease_states",
      ] as const;
      const before = new Map<string, number>();
      for (const table of tables) {
        before.set(
          table,
          (yield* sql.unsafe<{ readonly count: number }>(
            `SELECT COUNT(*) AS count FROM ${table}`,
          ))[0]!.count,
        );
      }

      yield* runMigrations({ toMigrationInclusive: 46 });

      assert.equal(
        (yield* sql<{ readonly sequence: number }>`
          SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.sequence,
        sequenceBefore,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM agent_control_events WHERE event_id = 'event-before-046'`,
        eventBefore,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT * FROM agent_control_command_receipts
          WHERE command_id = 'command-before-046'
        `,
        receiptBefore,
      );

      for (const table of tables) {
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
          FROM pragma_foreign_key_list('agent_control_worktree_reservation_states')
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list('agent_control_worktree_controller_operations')
        `)[0]!.count,
        0,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM pragma_index_info('idx_agent_control_worktree_task_stage')
          ORDER BY seqno
        `,
        [
          { name: "project_id" },
          { name: "task_id" },
          { name: "stage_run_id" },
          { name: "attempt_id" },
        ],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM pragma_index_info('idx_agent_control_worktree_branch')
          ORDER BY seqno
        `,
        [{ name: "repository_canonical_key" }, { name: "branch_name" }],
      );

      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-worktree-046', 'worktree-reservation', 'reservation-046', 1,
          'agentControl.worktree.reserved', ${at}, 'command-worktree-046',
          NULL, 'command-worktree-046', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_worktree_controller_operations (
          command_id, command_type, input_fingerprint, project_id, task_id,
          reservation_id, worktree_reservation_id, status, result_json,
          rejection_code, created_at, updated_at, completed_at
        ) VALUES (
          'composite-command-046', 'reserve-and-materialize', ${"a".repeat(64)},
          'project-046', 'task-046', NULL, NULL, 'pending', NULL, NULL,
          ${at}, ${at}, NULL
        )
      `;
      for (const invalid of [
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            status, created_at, updated_at
          ) VALUES (
            'composite-invalid-fingerprint', 'reserve-and-materialize', 'not-sha256',
            'project-046', 'task-046', 'pending', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id,
            status, created_at, updated_at
          ) VALUES (
            'composite-reserve-without-task', 'reserve-and-materialize',
            ${"b".repeat(64)}, 'project-046', 'pending', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id,
            status, created_at, updated_at
          ) VALUES (
            'composite-reconcile-without-reservation', 'reconcile',
            ${"c".repeat(64)}, 'project-046', 'pending', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            status, pending_token, created_at, updated_at
          ) VALUES (
            'composite-partial-claim', 'reserve-and-materialize',
            ${"d".repeat(64)}, 'project-046', 'task-046', 'pending',
            'token-only', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            worktree_reservation_id, status, result_json,
            completed_at, materialization_phase, created_at, updated_at
          ) VALUES (
            'composite-incomplete-accepted', 'reserve-and-materialize',
            ${"e".repeat(64)}, 'project-046', 'task-046', 'reservation-046',
            'accepted', '{}', ${at}, 'terminal', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            status, rejection_code, result_json, completed_at,
            materialization_phase, created_at, updated_at
          ) VALUES (
            'composite-mixed-rejected', 'reserve-and-materialize',
            ${"f".repeat(64)}, 'project-046', 'task-046', 'rejected',
            'validation', '{}', ${at}, 'terminal', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            status, created_at, updated_at
          ) VALUES (
            'agent-control-internal-worktree-v1-forged', 'reserve-and-materialize',
            ${"a".repeat(64)}, 'project-046', 'task-046', 'pending', ${at}, ${at}
          )
        `,
      ]) {
        assert.equal((yield* Effect.result(invalid))._tag, "Failure");
      }
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-worktree-rejected-046', 'fingerprint-worktree-046', 'controller',
          'worktree-reservation', 'reservation-046', 'rejected', 1, 1, 0, ${at},
          'lease-expired'
        )
      `;
    }),
  );
});

const rollbackLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

rollbackLayer("046_AgentControlWorktreeReservationFoundation rollback", (it) => {
  it.effect("rolls back the complete migration when a later table conflicts", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 45 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-rollback-046', 'stage-run-lease', 'lease-rollback-046', 1,
          'agentControl.stageRunLease.reserved', ${at}, 'command-rollback-046',
          NULL, 'command-rollback-046', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      const sequenceBefore = (yield* sql<{ readonly sequence: number }>`
        SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
      `)[0]!.sequence;
      yield* sql`
        CREATE TABLE agent_control_worktree_reservation_states (
          sentinel TEXT PRIMARY KEY
        )
      `;
      yield* sql`
        INSERT INTO agent_control_worktree_reservation_states (sentinel)
        VALUES ('must-survive')
      `;

      const migrated = yield* Effect.exit(runMigrations({ toMigrationInclusive: 46 }));
      assert.equal(migrated._tag, "Failure");
      assert.deepStrictEqual(
        yield* sql`
          SELECT event_id, aggregate_kind, stream_id
          FROM agent_control_events
          WHERE event_id = 'event-rollback-046'
        `,
        [
          {
            event_id: "event-rollback-046",
            aggregate_kind: "stage-run-lease",
            stream_id: "lease-rollback-046",
          },
        ],
      );
      assert.equal(
        (yield* sql<{ readonly sequence: number }>`
          SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.sequence,
        sequenceBefore,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT sentinel FROM agent_control_worktree_reservation_states
        `,
        [{ sentinel: "must-survive" }],
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM sqlite_master
          WHERE type = 'table'
            AND name = 'agent_control_worktree_controller_operations'
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_sql_migrations
          WHERE migration_id = 46
        `)[0]!.count,
        0,
      );
    }),
  );
});
