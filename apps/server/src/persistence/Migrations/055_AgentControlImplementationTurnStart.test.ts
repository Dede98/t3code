import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration055 from "./055_AgentControlImplementationTurnStart.ts";

it.live(
  "installs the durable implementation materialization, delivery, and stage-start boundary atomically",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-implementation-turn-migration-",
        });
        const filename = path.join(directory, "state.sqlite");
        const scopeA = yield* Scope.make("sequential");
        const scopeB = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
        const contextA = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeA);
        const contextB = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeB);
        const sqlA = Context.get(contextA, SqlClient.SqlClient);
        const sqlB = Context.get(contextB, SqlClient.SqlClient);

        for (const sql of [sqlA, sqlB]) {
          assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
          yield* sql`PRAGMA foreign_keys = ON`;
          assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
        }

        yield* runMigrations({ toMigrationInclusive: 54 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sqlA),
        );
        const beforeSchema = yield* sqlA<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM sqlite_schema ORDER BY type, name
        `;
        const beforeSequence = yield* sqlA<Record<string, unknown>>`
          SELECT name, seq FROM sqlite_sequence ORDER BY name
        `;
        const beforeMigrations = yield* sqlA<Record<string, unknown>>`
          SELECT * FROM effect_sql_migrations ORDER BY migration_id
        `;
        const rollback = yield* Effect.exit(
          sqlA.withTransaction(
            Effect.gen(function* () {
              yield* Migration055.pipe(Effect.provideService(SqlClient.SqlClient, sqlA));
              yield* sqlA.unsafe("CREATE TABLE migration_055_broken(").unprepared;
            }),
          ),
        );
        assert.equal(Exit.isFailure(rollback), true);
        assert.deepStrictEqual(
          yield* sqlA<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM sqlite_schema ORDER BY type, name
          `,
          beforeSchema,
        );
        assert.deepStrictEqual(
          yield* sqlA<Record<string, unknown>>`
            SELECT name, seq FROM sqlite_sequence ORDER BY name
          `,
          beforeSequence,
        );
        assert.deepStrictEqual(
          yield* sqlA<Record<string, unknown>>`
            SELECT * FROM effect_sql_migrations ORDER BY migration_id
          `,
          beforeMigrations,
        );

        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 55 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlA),
          ),
          [[55, "AgentControlImplementationTurnStart"] as const],
        );
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 55 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          ),
          [],
        );

        const tables = yield* sqlB<{ readonly name: string }>`
          SELECT name FROM sqlite_schema
          WHERE type = 'table' AND name IN (
            'agent_control_implementation_materialization_evidence',
            'agent_control_implementation_materialization_receipts',
            'agent_control_implementation_materialization_markers',
            'agent_control_implementation_handoff_intents',
            'agent_control_implementation_handoff_receipts',
            'agent_control_implementation_handoff_accepted',
            'agent_control_implementation_deliveries',
            'agent_control_implementation_turn_accepted',
            'agent_control_implementation_session_evidence',
            'agent_control_implementation_delivery_attestations',
            'agent_control_implementation_stage_started_evidence',
            'agent_control_implementation_stage_started_receipts',
            'agent_control_implementation_stage_started_markers'
          ) ORDER BY name
        `;
        assert.equal(tables.length, 13);
        assert.deepStrictEqual(
          yield* sqlB<{ readonly name: string }>`
            SELECT name FROM pragma_table_info(
              'agent_control_implementation_materialization_evidence'
            ) WHERE name IN (
              'repository_display', 'source_revision', 'task_title', 'task_body'
            ) ORDER BY name
          `,
          [
            { name: "repository_display" },
            { name: "source_revision" },
            { name: "task_body" },
            { name: "task_title" },
          ],
        );
        assert.deepStrictEqual(
          yield* sqlB<{ readonly name: string }>`
            SELECT name FROM sqlite_schema
            WHERE type = 'trigger' AND name IN (
              'agent_control_implementation_materialization_evidence_validate',
              'agent_control_implementation_handoff_intent_validate'
            ) ORDER BY name
          `,
          [
            { name: "agent_control_implementation_handoff_intent_validate" },
            { name: "agent_control_implementation_materialization_evidence_validate" },
          ],
        );
        assert.deepStrictEqual(yield* sqlB`PRAGMA foreign_key_check`, []);
        assert.deepStrictEqual(yield* sqlB`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
