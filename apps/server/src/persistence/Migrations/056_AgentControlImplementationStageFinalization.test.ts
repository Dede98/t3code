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
import Migration056 from "./056_AgentControlImplementationStageFinalization.ts";

it.live(
  "upgrades a populated Migration-055 WAL database and rolls the schema back atomically",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-implementation-finalization-migration-",
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

        yield* runMigrations({ toMigrationInclusive: 55 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sqlA),
        );
        yield* sqlA`
          INSERT INTO projection_projects (
            project_id, title, workspace_root, default_model_selection_json,
            scripts_json, created_at, updated_at, deleted_at
          ) VALUES (
            'migration-056-project', 'Migration 056', '/tmp/migration-056', NULL,
            '[]', '2026-08-05T00:00:00.000Z', '2026-08-05T00:00:00.000Z', NULL
          )
        `;
        const schemaBefore = yield* sqlA<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM sqlite_schema ORDER BY type, name
        `;
        const sequenceBefore = yield* sqlA<Record<string, unknown>>`
          SELECT name, seq FROM sqlite_sequence ORDER BY name
        `;
        const rollback = yield* Effect.exit(
          sqlA.withTransaction(
            Effect.gen(function* () {
              yield* Migration056.pipe(Effect.provideService(SqlClient.SqlClient, sqlA));
              yield* sqlA.unsafe("CREATE TABLE migration_056_broken(").unprepared;
            }),
          ),
        );
        assert.isTrue(Exit.isFailure(rollback));
        assert.deepStrictEqual(
          yield* sqlA<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM sqlite_schema ORDER BY type, name
          `,
          schemaBefore,
        );
        assert.deepStrictEqual(
          yield* sqlA<Record<string, unknown>>`
            SELECT name, seq FROM sqlite_sequence ORDER BY name
          `,
          sequenceBefore,
        );

        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 56 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlA),
          ),
          [[56, "AgentControlImplementationStageFinalization"] as const],
        );
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 56 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          ),
          [],
        );

        assert.deepStrictEqual(
          yield* sqlB<{ readonly title: string }>`
            SELECT title FROM projection_projects WHERE project_id = 'migration-056-project'
          `,
          [{ title: "Migration 056" }],
        );
        const tables = yield* sqlB<{ readonly name: string }>`
          SELECT name FROM sqlite_schema
          WHERE type = 'table' AND name IN (
            'agent_control_implementation_result_evidence',
            'agent_control_implementation_stage_finalization_receipts',
            'agent_control_implementation_stage_finalization_markers'
          ) ORDER BY name
        `;
        assert.deepStrictEqual(tables, [
          { name: "agent_control_implementation_result_evidence" },
          { name: "agent_control_implementation_stage_finalization_markers" },
          { name: "agent_control_implementation_stage_finalization_receipts" },
        ]);

        const eventTable = yield* sqlB<{ readonly sql: string }>`
          SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'agent_control_events'
        `;
        assert.equal(eventTable.length, 1);
        for (const eventType of [
          "agentControl.stageRun.implementationSucceeded",
          "agentControl.stageRun.implementationFailed",
          "agentControl.stageRun.implementationCancelled",
          "agentControl.stageRunLease.releasedAfterImplementation",
        ]) {
          assert.include(eventTable[0]!.sql, eventType);
        }

        const triggerCount = yield* sqlB<{ readonly count: number }>`
          SELECT count(*) AS count FROM sqlite_schema
          WHERE type = 'trigger' AND name LIKE 'agent_control_implementation%finalization%'
        `;
        assert.isAbove(triggerCount[0]!.count, 8);

        for (const malformed of [
          "INSERT INTO agent_control_implementation_result_evidence(result_evidence_id) VALUES ('partial')",
          "INSERT INTO agent_control_implementation_stage_finalization_receipts(receipt_id) VALUES (CAST(X'80' AS TEXT))",
          "INSERT INTO agent_control_implementation_stage_finalization_markers(marker_id) VALUES (X'01')",
        ]) {
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(sqlB.withTransaction(sqlB.unsafe(malformed)))),
          );
        }
        assert.deepStrictEqual(yield* sqlB`PRAGMA foreign_key_check`, []);
        assert.deepStrictEqual(yield* sqlB`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
