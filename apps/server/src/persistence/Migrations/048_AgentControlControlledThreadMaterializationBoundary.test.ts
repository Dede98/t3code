import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const rollbackLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("048_AgentControlControlledThreadMaterializationBoundary", (it) => {
  it.effect(
    "preserves existing orchestration data, indexes and sequence and is a no-op twice",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations({ toMigrationInclusive: 47 });
        yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'event-before-048', 'project', 'project-before-048', 0,
          'project.created', '2026-07-27T10:00:00.000Z',
          'command-before-048', NULL, 'command-before-048', 'server',
          '{"projectId":"project-before-048","title":"Before","workspaceRoot":"/tmp/before","defaultModelSelection":null,"scripts":[],"createdAt":"2026-07-27T10:00:00.000Z","updatedAt":"2026-07-27T10:00:00.000Z"}',
          '{}'
        )
      `;
        yield* sql`
        INSERT INTO orchestration_command_receipts (
          command_id, authority, aggregate_kind, aggregate_id, accepted_at,
          result_sequence, status, error
        ) VALUES (
          'command-before-048', 'system', 'project', 'project-before-048',
          '2026-07-27T10:00:00.000Z', 1, 'accepted', NULL
        )
      `;
        yield* sql`
        CREATE INDEX preserved_orchestration_index_before_048
        ON orchestration_events(correlation_id)
      `;
        const beforeSequence = yield* sql<{ readonly seq: number }>`
        SELECT seq FROM sqlite_sequence WHERE name = 'orchestration_events'
      `;

        const first = yield* runMigrations({ toMigrationInclusive: 48 });
        const second = yield* runMigrations({ toMigrationInclusive: 48 });
        assert.deepStrictEqual(
          first.map(([id]) => id),
          [48],
        );
        assert.deepStrictEqual(second, []);

        const rows = yield* sql<{ readonly events: number; readonly receipts: number }>`
        SELECT
          (SELECT COUNT(*) FROM orchestration_events
           WHERE event_id = 'event-before-048') AS events,
          (SELECT COUNT(*) FROM orchestration_command_receipts
           WHERE command_id = 'command-before-048') AS receipts
      `;
        assert.deepStrictEqual(rows, [{ events: 1, receipts: 1 }]);
        assert.deepStrictEqual(
          yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM sqlite_schema
          WHERE type = 'index'
            AND name = 'preserved_orchestration_index_before_048'
        `,
          [{ count: 1 }],
        );
        assert.deepStrictEqual(
          yield* sql<{ readonly seq: number }>`
          SELECT seq FROM sqlite_sequence WHERE name = 'orchestration_events'
        `,
          beforeSequence,
        );
        const indexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_schema
        WHERE type = 'index'
          AND name IN (
            'idx_orchestration_events_materialization_coordinates',
            'idx_orchestration_receipts_materialization_coordinates',
            'idx_orchestration_materialization_thread',
            'idx_orchestration_materialization_reservation'
          )
        ORDER BY name
      `;
        assert.lengthOf(indexes, 4);
      }),
  );

  it.effect("keeps intents immutable and introduces no projection foreign key", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations();
      const foreignKeys = yield* sql<{ readonly table: string }>`
        PRAGMA foreign_key_list(orchestration_agent_control_thread_materialization_intents)
      `;
      assert.deepStrictEqual(
        new Set(foreignKeys.map((foreignKey) => foreignKey.table)),
        new Set(["orchestration_events", "orchestration_command_receipts"]),
      );

      yield* sql`
        INSERT INTO orchestration_command_receipts (
          command_id, authority, aggregate_kind, aggregate_id, accepted_at,
          result_sequence, status, error
        ) VALUES (
          'rejected-048', 'agent-control', 'thread', 'thread-rejected-048',
          '2026-07-27T10:00:00.000Z', 0, 'rejected', 'rejected'
        )
      `;
      yield* sql`
        INSERT INTO orchestration_agent_control_thread_materialization_intents (
          command_id, command_type, authority, aggregate_kind, command_fingerprint,
          controlled_thread_reservation_id, thread_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
          attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
          title, model_selection_json, runtime_mode, interaction_mode, branch,
          worktree_path, binding_json, created_event_id, created_event_type,
          created_event_sequence, created_event_stream_version, binding_event_id,
          binding_event_type, binding_event_sequence, binding_event_stream_version,
          receipt_status, receipt_result_sequence, receipt_accepted_at,
          receipt_error, created_at
        ) VALUES (
          'rejected-048', 'thread.agent-control.materialize', 'agent-control',
          'thread', ${"a".repeat(64)}, 'reservation-rejected-048',
          'thread-rejected-048', 'project-rejected-048', 'task-rejected-048',
          1, 1, ${"b".repeat(64)}, 'stage-rejected-048', 'attempt-rejected-048',
          'planning', 'planning', 1, 1, 'lease-rejected-048', 1,
          'worktree-rejected-048', 'Rejected', '{"instanceId":"codex","model":"gpt"}',
          'approval-required', 'plan', 'branch-rejected-048',
          '/tmp/rejected-048',
          '{"taskId":"task-rejected-048","stageRunId":"stage-rejected-048","attemptId":"attempt-rejected-048","roleId":"planning","controlState":"controlled"}',
          NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
          'rejected', 0, '2026-07-27T10:00:00.000Z', 'rejected',
          '2026-07-27T10:00:00.000Z'
        )
      `;
      assert.strictEqual(
        Exit.isFailure(
          yield* Effect.exit(
            sql`
              UPDATE orchestration_agent_control_thread_materialization_intents
              SET title = 'Changed' WHERE command_id = 'rejected-048'
            `,
          ),
        ),
        true,
      );
      assert.strictEqual(
        Exit.isFailure(
          yield* Effect.exit(
            sql`
              DELETE FROM orchestration_agent_control_thread_materialization_intents
              WHERE command_id = 'rejected-048'
            `,
          ),
        ),
        true,
      );
    }),
  );
});

rollbackLayer("048_AgentControlControlledThreadMaterializationBoundary rollback", (it) => {
  it.effect("rolls the entire additive migration back when later DDL fails", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 47 });
      yield* sql`
        CREATE TRIGGER trg_orchestration_materialization_intent_immutable_delete
        BEFORE INSERT ON orchestration_events
        BEGIN
          SELECT 1;
        END
      `;
      const exit = yield* Effect.exit(runMigrations({ toMigrationInclusive: 48 }));
      assert.strictEqual(Exit.isFailure(exit), true);
      const objects = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_schema
        WHERE name IN (
          'orchestration_agent_control_thread_materialization_intents',
          'idx_orchestration_events_materialization_coordinates',
          'idx_orchestration_receipts_materialization_coordinates',
          'idx_orchestration_materialization_thread',
          'idx_orchestration_materialization_reservation',
          'trg_orchestration_materialization_intent_immutable_update'
        )
      `;
      assert.deepStrictEqual(objects, []);
      const migrationRows = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_sql_migrations WHERE migration_id = 48
      `;
      assert.deepStrictEqual(migrationRows, [{ count: 0 }]);
    }),
  );
});
