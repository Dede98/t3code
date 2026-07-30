import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration050 from "./050_AgentControlControlledThreadPrepareFinalization.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const rollbackLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("050_AgentControlControlledThreadPrepareFinalization", (it) => {
  it.effect("is data-preserving, idempotent, and leaves legacy receipts unbackfilled", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 49 });
      const beforeSequence = yield* sql<{ readonly name: string; readonly seq: number }>`
        SELECT name, seq FROM sqlite_sequence ORDER BY name
      `;
      const beforeIndexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_schema
        WHERE type = 'index'
          AND name NOT LIKE 'idx_agent_control_controlled_thread_prepare_%'
          AND name NOT LIKE
            'sqlite_autoindex_agent_control_controlled_thread_prepare_finalizations_%'
        ORDER BY name
      `;

      assert.deepStrictEqual(yield* runMigrations(), [
        [50, "AgentControlControlledThreadPrepareFinalization"],
      ]);
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string; readonly seq: number }>`
          SELECT name, seq FROM sqlite_sequence ORDER BY name
        `,
        beforeSequence,
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string }>`
          SELECT name FROM sqlite_schema
          WHERE type = 'index'
            AND name NOT LIKE 'idx_agent_control_controlled_thread_prepare_%'
            AND name NOT LIKE
              'sqlite_autoindex_agent_control_controlled_thread_prepare_finalizations_%'
          ORDER BY name
        `,
        beforeIndexes,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT COUNT(*) AS count
          FROM agent_control_controlled_thread_prepare_finalizations
        `,
        [{ count: 0 }],
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list(
            'agent_control_controlled_thread_prepare_finalizations'
          )
          WHERE "table" LIKE 'projection_%'
        `)[0]!.count,
        0,
      );
    }),
  );

  it.effect("rejects partial evidence without leaving an outbox row", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const failed = yield* Effect.exit(
        sql.withTransaction(
          sql`
            INSERT INTO agent_control_controlled_thread_prepare_finalizations (
              prepare_command_id, prepare_command_fingerprint,
              project_id, task_id, controlled_thread_reservation_id,
              prepared_event_id, prepared_stream_version,
              prepared_event_sequence, receipt_command_id, receipt_status,
              receipt_result_sequence, receipt_result_stream_version,
              receipt_event_created, receipt_accepted_at,
              finalization_owner_id, status, revision, claimed_at
            ) VALUES (
              'prepare-050-missing', ${"a".repeat(64)},
              'project-050', 'task-050', 'reservation-050',
              'event-050', 1, 1, 'prepare-050-missing', 'accepted',
              1, 1, 1, '2026-07-30T08:00:00.000Z',
              '00000000-0000-0000-0000-000000000050',
              'claimed', 1, '2026-07-30T08:00:00.000Z'
            )
          `,
        ),
      );
      assert.equal(Exit.isFailure(failed), true);
      assert.deepStrictEqual(
        yield* sql`
          SELECT COUNT(*) AS count
          FROM agent_control_controlled_thread_prepare_finalizations
        `,
        [{ count: 0 }],
      );
    }),
  );
});

rollbackLayer("050 prepare finalization rollback", (it) => {
  it.effect("rolls back earlier DDL when a later DDL statement fails", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 49 });
      yield* sql`
        CREATE TABLE migration_050_index_collision (collision INTEGER)
      `;
      yield* sql`
        CREATE INDEX
          idx_agent_control_controlled_thread_prepare_finalizations_open
        ON migration_050_index_collision(collision)
      `;
      const failed = yield* Effect.exit(sql.withTransaction(Migration050));
      assert.equal(Exit.isFailure(failed), true);
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM sqlite_schema
          WHERE type = 'table'
            AND name =
              'agent_control_controlled_thread_prepare_finalizations'
          ORDER BY name
        `,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM sqlite_schema
          WHERE type = 'index'
            AND name =
              'idx_agent_control_controlled_thread_prepare_finalizations_open'
        `,
        [
          {
            name: "idx_agent_control_controlled_thread_prepare_finalizations_open",
          },
        ],
      );
    }),
  );
});
