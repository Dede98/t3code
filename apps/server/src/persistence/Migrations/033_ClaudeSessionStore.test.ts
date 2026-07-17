import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("033_ClaudeSessionStore", (it) => {
  it.effect("upgrades an existing schema 32 database with the additive session store", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 32 });
      const tablesBefore = yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name LIKE 'claude_session_store_%'
        ORDER BY name
      `;
      assert.deepStrictEqual(tablesBefore, []);

      yield* runMigrations({ toMigrationInclusive: 33 });

      const tablesAfter = yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name LIKE 'claude_session_store_%'
        ORDER BY name
      `;
      assert.deepStrictEqual(
        tablesAfter.map((table) => table.name),
        ["claude_session_store_clock", "claude_session_store_entries", "claude_session_store_keys"],
      );

      const migration = yield* sql<{
        readonly migrationId: number;
        readonly name: string;
      }>`
        SELECT migration_id AS "migrationId", name
        FROM effect_sql_migrations
        WHERE migration_id = 33
      `;
      assert.deepStrictEqual(migration, [{ migrationId: 33, name: "ClaudeSessionStore" }]);

      const clock = yield* sql<{ readonly lastMtimeMs: number }>`
        SELECT last_mtime_ms AS "lastMtimeMs"
        FROM claude_session_store_clock
        WHERE singleton = 1
      `;
      assert.deepStrictEqual(clock, [{ lastMtimeMs: 0 }]);
    }),
  );
});
