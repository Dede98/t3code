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
import { VERIFICATION_STAGE_FINALIZATION_CANDIDATES_SQL } from "../../agentControl/verificationTurn/Layers/AgentControlVerificationHandoffStore.ts";
import {
  makeMigration061,
  type Migration061FaultPoint,
} from "./061_AgentControlVerificationStageFinalization.ts";

const openDatabase = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make("sequential");
    const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
    const sql = Context.get(context, SqlClient.SqlClient);
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
    return { scope, sql } as const;
  });

it.live("installs Verification finalization atomically on fresh and populated WAL databases", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-finalization-migration-",
      });
      const upgrade = yield* openDatabase(path.join(directory, "upgrade.sqlite"));
      const observer = yield* openDatabase(path.join(directory, "upgrade.sqlite"));
      yield* Effect.addFinalizer(() => Scope.close(observer.scope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(upgrade.scope, Exit.void));

      yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
        Effect.provideService(SqlClient.SqlClient, upgrade.sql),
      );
      yield* upgrade.sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          scripts_json, created_at, updated_at, deleted_at
        ) VALUES (
          'migration-061-project', 'Migration 061', '/tmp/migration-061', NULL,
          '[]', '2026-08-29T00:00:00.000Z', '2026-08-29T00:00:00.000Z', NULL
        )
      `;
      const schemaBefore = yield* upgrade.sql<Record<string, unknown>>`
        SELECT type, name, tbl_name AS "tableName", sql
        FROM main.sqlite_schema ORDER BY type, name
      `;
      const sequenceBefore = yield* upgrade.sql<Record<string, unknown>>`
        SELECT name, seq FROM main.sqlite_sequence ORDER BY name
      `;

      for (const faultPoint of [
        "before-events-rebuild",
        "after-events-rebuild",
        "after-companions",
        "after-install",
      ] satisfies ReadonlyArray<Migration061FaultPoint>) {
        const rollback = yield* Effect.exit(
          upgrade.sql.withTransaction(
            makeMigration061(faultPoint).pipe(
              Effect.provideService(SqlClient.SqlClient, upgrade.sql),
            ),
          ),
        );
        assert.isTrue(Exit.isFailure(rollback), faultPoint);
        assert.deepStrictEqual(
          yield* upgrade.sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
          schemaBefore,
          faultPoint,
        );
        assert.deepStrictEqual(
          yield* upgrade.sql<Record<string, unknown>>`
            SELECT name, seq FROM main.sqlite_sequence ORDER BY name
          `,
          sequenceBefore,
          faultPoint,
        );
      }

      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
          Effect.provideService(SqlClient.SqlClient, upgrade.sql),
        ),
        [[61, "AgentControlVerificationStageFinalization"] as const],
      );
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
          Effect.provideService(SqlClient.SqlClient, observer.sql),
        ),
        [],
      );
      assert.deepStrictEqual(
        yield* observer.sql`
          SELECT title FROM projection_projects WHERE project_id = 'migration-061-project'
        `,
        [{ title: "Migration 061" }],
      );
      assert.deepStrictEqual(
        yield* observer.sql<{ readonly name: string }>`
          SELECT name FROM main.sqlite_schema
          WHERE type = 'table' AND name LIKE 'agent_control_verification_finalization_%'
          ORDER BY name
        `,
        [
          { name: "agent_control_verification_finalization_evidence" },
          { name: "agent_control_verification_finalization_markers" },
          { name: "agent_control_verification_finalization_receipts" },
        ],
      );
      const eventSchema = yield* observer.sql<{ readonly sql: string }>`
        SELECT sql FROM main.sqlite_schema
        WHERE type = 'table' AND name = 'agent_control_events'
      `;
      assert.equal(eventSchema.length, 1);
      for (const eventType of [
        "agentControl.stageRun.verificationSucceeded",
        "agentControl.stageRun.verificationFailed",
        "agentControl.stageRun.verificationCancelled",
        "agentControl.stageRunLease.releasedAfterVerification",
      ]) {
        assert.include(eventSchema[0]!.sql, eventType);
      }
      assert.isAbove(
        (yield* observer.sql<{ readonly count: number }>`
            SELECT count(*) AS count FROM main.sqlite_schema
            WHERE type = 'trigger' AND name LIKE 'agent_control_verification_finalization_%'
          `)[0]!.count,
        8,
      );
      assert.deepStrictEqual(yield* observer.sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* observer.sql`PRAGMA integrity_check`, [
        { integrity_check: "ok" },
      ]);
      const plan = yield* observer.sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${VERIFICATION_STAGE_FINALIZATION_CANDIDATES_SQL}`,
        ["", 64],
      );
      assert.isFalse(plan.some(({ detail }) => detail.includes("USE TEMP B-TREE")));
      assert.isTrue(
        plan.some(({ detail }) =>
          detail.includes("idx_agent_control_verification_finalization_candidates"),
        ),
      );
      assert.isTrue(
        plan.some(
          ({ detail }) =>
            detail.includes("agent_control_verification_finalization_markers") &&
            detail.includes("INDEX"),
        ),
      );

      const fresh = yield* openDatabase(path.join(directory, "fresh.sqlite"));
      yield* Effect.addFinalizer(() => Scope.close(fresh.scope, Exit.void));
      assert.equal(
        (yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
          Effect.provideService(SqlClient.SqlClient, fresh.sql),
        )).at(-1)?.[0],
        61,
      );
      assert.deepStrictEqual(yield* fresh.sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* fresh.sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
