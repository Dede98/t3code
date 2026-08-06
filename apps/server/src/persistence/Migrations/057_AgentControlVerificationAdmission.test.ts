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
import Migration057 from "./057_AgentControlVerificationAdmission.ts";

const openWal = Effect.fn("openVerificationAdmissionMigrationDatabase")(function* (
  filename: string,
  scope: Scope.Closeable,
) {
  const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
  const sql = Context.get(context, SqlClient.SqlClient);
  assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
  yield* sql`PRAGMA foreign_keys = ON`;
  assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
  return sql;
});

it.live(
  "installs Migration 057 fresh and from 056 while preserving data and rolling back late failure",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-verification-admission-migration-",
        });
        const upgradeFilename = path.join(directory, "upgrade.sqlite");
        const freshFilename = path.join(directory, "fresh.sqlite");
        const scopeA = yield* Scope.make("sequential");
        const scopeB = yield* Scope.make("sequential");
        const scopeFresh = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scopeFresh, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));

        const sqlA = yield* openWal(upgradeFilename, scopeA);
        const sqlB = yield* openWal(upgradeFilename, scopeB);
        yield* runMigrations({ toMigrationInclusive: 56 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sqlA),
        );
        yield* sqlA`
          INSERT INTO projection_projects (
            project_id, title, workspace_root, default_model_selection_json,
            scripts_json, created_at, updated_at, deleted_at
          ) VALUES (
            'migration-057-project', 'Migration 057', '/tmp/migration-057', NULL,
            '[]', '2026-08-06T00:00:00.000Z', '2026-08-06T00:00:00.000Z', NULL
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
              yield* Migration057.pipe(Effect.provideService(SqlClient.SqlClient, sqlA));
              yield* sqlA.unsafe("CREATE TABLE migration_057_broken(").unprepared;
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
          yield* runMigrations({ toMigrationInclusive: 57 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlA),
          ),
          [[57, "AgentControlVerificationAdmission"] as const],
        );
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 57 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          ),
          [],
        );
        assert.deepStrictEqual(
          yield* sqlB<{ readonly title: string }>`
            SELECT title FROM projection_projects WHERE project_id = 'migration-057-project'
          `,
          [{ title: "Migration 057" }],
        );

        const objects = yield* sqlB<{ readonly name: string; readonly type: string }>`
          SELECT name, type FROM sqlite_schema
          WHERE name IN (
            'agent_control_verification_thread_stream_catalog',
            'agent_control_verification_thread_reservation_states',
            'agent_control_controlled_thread_stream_catalog_all',
            'agent_control_controlled_thread_reservation_states_all',
            'agent_control_verification_admission_evidence',
            'agent_control_verification_admission_receipts',
            'agent_control_verification_admission_markers'
          ) ORDER BY name
        `;
        assert.equal(objects.length, 7);
        assert.equal(objects.filter((object) => object.type === "table").length, 5);
        assert.equal(objects.filter((object) => object.type === "view").length, 2);
        const views = yield* sqlB<{ readonly name: string; readonly sql: string }>`
          SELECT name, sql FROM sqlite_schema
          WHERE type = 'view' AND name IN (
            'agent_control_controlled_thread_stream_catalog_all',
            'agent_control_controlled_thread_reservation_states_all'
          ) ORDER BY name
        `;
        assert.lengthOf(views, 2);
        for (const view of views) {
          assert.include(view.sql, "agent_control_verification_thread_");
        }
        const totalGuard = yield* sqlB<{ readonly sql: string }>`
          SELECT sql FROM sqlite_schema WHERE type = 'trigger'
            AND name = 'agent_control_controlled_thread_event_total_validate'
        `;
        assert.lengthOf(totalGuard, 1);
        assert.include(totalGuard[0]!.sql, "NOT COALESCE");
        assert.include(totalGuard[0]!.sql, "stageKind') = 'verification'");
        const verificationTriggers = yield* sqlB<{ readonly count: number }>`
          SELECT count(*) AS count FROM sqlite_schema
          WHERE type = 'trigger' AND name LIKE 'agent_control_verification_%'
        `;
        assert.isAbove(verificationTriggers[0]!.count, 18);

        for (const malformed of [
          "INSERT INTO agent_control_verification_admission_evidence(admission_evidence_id) VALUES ('partial')",
          "INSERT INTO agent_control_verification_admission_receipts(receipt_id) VALUES (CAST(X'80' AS TEXT))",
          "INSERT INTO agent_control_verification_thread_stream_catalog(controlled_thread_reservation_id) VALUES (X'01')",
        ]) {
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(sqlB.withTransaction(sqlB.unsafe(malformed)))),
          );
        }
        assert.deepStrictEqual(yield* sqlB`PRAGMA foreign_key_check`, []);
        assert.deepStrictEqual(yield* sqlB`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);

        const sqlFresh = yield* openWal(freshFilename, scopeFresh);
        assert.equal(
          (yield* runMigrations({ toMigrationInclusive: 57 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlFresh),
          )).at(-1)?.[0],
          57,
        );
        assert.deepStrictEqual(yield* sqlFresh`PRAGMA foreign_key_check`, []);
        assert.deepStrictEqual(yield* sqlFresh`PRAGMA integrity_check`, [
          { integrity_check: "ok" },
        ]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
