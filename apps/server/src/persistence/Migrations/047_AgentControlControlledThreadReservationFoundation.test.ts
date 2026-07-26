import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const rollbackLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const at = "2026-07-26T10:00:00.000Z";

layer("047_AgentControlControlledThreadReservationFoundation", (it) => {
  it.effect("preserves all prior data and adds isolated reservation constraints", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 46 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-047-stage', 'stage-run', 'stage-run-047', 1,
          'agentControl.stageRun.prepared', ${at}, 'command-047-stage',
          NULL, 'command-047-stage', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-047-stage', 'fingerprint-047-stage', 'controller', 'stage-run',
          'stage-run-047', 'accepted', 1, 1, 1, ${at}, NULL
        )
      `;
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'orchestration-event-047', 'project', 'project-047', 1,
          'project.created', ${at}, 'orchestration-command-047', NULL,
          'orchestration-command-047', 'client', '{}', '{}'
        )
      `;
      const beforeEventSequence = (yield* sql<{ readonly seq: number }>`
        SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
      `)[0]!.seq;
      const beforeOrchestration = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM orchestration_events
      `)[0]!.count;
      const beforeReceipts = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_command_receipts
      `)[0]!.count;
      const beforeWorktreeEventTriggers = yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_schema
        WHERE type = 'trigger' AND tbl_name = 'agent_control_events'
          AND name LIKE 'agent_control_worktree_event_%'
        ORDER BY name ASC
      `;

      yield* runMigrations({ toMigrationInclusive: 47 });

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM orchestration_events
        `)[0]!.count,
        beforeOrchestration,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
        `)[0]!.count,
        beforeReceipts,
      );
      assert.equal(
        (yield* sql<{ readonly seq: number }>`
          SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.seq,
        beforeEventSequence,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list(
            'agent_control_controlled_thread_reservation_states'
          )
        `)[0]!.count,
        0,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name
          FROM pragma_index_info(
            'idx_agent_control_controlled_thread_semantic_position'
          )
          ORDER BY seqno
        `,
        [
          { name: "project_id" },
          { name: "task_id" },
          { name: "stage_run_id" },
          { name: "attempt_id" },
          { name: "role_id" },
          { name: "stage_ordinal" },
          { name: "attempt_ordinal" },
        ],
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string }>`
          SELECT name
          FROM sqlite_schema
          WHERE type = 'trigger' AND tbl_name = 'agent_control_events'
            AND name LIKE 'agent_control_worktree_event_%'
          ORDER BY name ASC
        `,
        beforeWorktreeEventTriggers,
      );

      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-047-reservation', 'controlled-thread-reservation',
          'controlled-thread-reservation-047', 1,
          'agentControl.controlledThreadReservation.prepared', ${at},
          'command-047-reservation', NULL, 'command-047-reservation',
          'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-047-rejected', 'fingerprint-047', 'controller',
          'controlled-thread-reservation', 'controlled-thread-reservation-rejected-047',
          'rejected', 0, 0, 0, ${at},
          'controlled-thread-reservation-identity-conflict'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_controlled_thread_reservation_states (
          controlled_thread_reservation_id, thread_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
          attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
          status, revision, last_event_sequence, prepared_at, state_json
        ) VALUES (
          'controlled-thread-reservation-047', 't3-auto-reserved-thread-047',
          'project-047', 'task-047', 1, 1,
          'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          'stage-run-047', 'attempt-047', 'planning', 'planning', 1, 1,
          'lease-047', 1, 'worktree-047', 'prepared', 1, 2, ${at}, '{}'
        )
      `;
      const competing = yield* Effect.result(sql`
        INSERT INTO agent_control_controlled_thread_reservation_states (
          controlled_thread_reservation_id, thread_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
          attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
          status, revision, last_event_sequence, prepared_at, state_json
        ) VALUES (
          'controlled-thread-reservation-047-other',
          't3-auto-reserved-thread-047-other', 'project-047', 'task-047', 1, 1,
          'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          'stage-run-047', 'attempt-047', 'planning', 'planning', 1, 1,
          'lease-other', 2, 'worktree-other', 'prepared', 1, 3, ${at}, '{}'
        )
      `);
      assert.equal(competing._tag, "Failure");

      const invalidEvent = yield* Effect.result(sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-047-invalid', 'controlled-thread-reservation',
          'controlled-thread-reservation-invalid', 1, 'thread.created', ${at},
          'command-047-invalid', NULL, 'command-047-invalid',
          'controller', '{}', '{"schemaVersion":1}'
        )
      `);
      assert.equal(invalidEvent._tag, "Failure");

      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 47 }), []);
    }),
  );
});

rollbackLayer("047_AgentControlControlledThreadReservationFoundation rollback", (it) => {
  it.effect("rolls both additive constraint rebuilds back when the final table conflicts", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 46 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-047-rollback-stage', 'stage-run', 'stage-run-047-rollback', 1,
          'agentControl.stageRun.prepared', ${at}, 'command-047-rollback-stage',
          NULL, 'command-047-rollback-stage', 'controller',
          '{}', '{"schemaVersion":1}'
        )
      `;
      const beforeSequence = (yield* sql<{ readonly seq: number }>`
        SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
      `)[0]!.seq;
      yield* sql`
        CREATE TABLE agent_control_controlled_thread_reservation_states (
          conflicting_column TEXT
        )
      `;

      const failed = yield* Effect.exit(runMigrations({ toMigrationInclusive: 47 }));
      assert.equal(Exit.isFailure(failed), true);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM agent_control_events
          WHERE event_id = 'event-047-rollback-stage'
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* sql<{ readonly seq: number }>`
          SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.seq,
        beforeSequence,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM effect_sql_migrations
          WHERE migration_id = 47
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_table_info('agent_control_events')
          WHERE name = 'aggregate_kind'
        `)[0]!.count,
        1,
      );
    }),
  );
});
