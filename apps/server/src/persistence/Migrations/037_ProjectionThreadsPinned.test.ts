import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("037_ProjectionThreadsPinned", (it) => {
  it.effect("appends pin state after the fork title migration", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 36 });
      const columnsBefore = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_threads)
      `;
      const namesBefore = new Set(columnsBefore.map((column) => column.name));
      assert.ok(namesBefore.has("title_regeneration_request_id"));
      assert.ok(namesBefore.has("title_regeneration_started_at"));
      assert.ok(!namesBefore.has("pinned_at"));

      yield* runMigrations({ toMigrationInclusive: 37 });
      const columnsAfter = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_threads)
      `;
      assert.ok(columnsAfter.some((column) => column.name === "pinned_at"));

      const migration = yield* sql<{
        readonly migrationId: number;
        readonly name: string;
      }>`
        SELECT migration_id AS "migrationId", name
        FROM effect_sql_migrations
        WHERE migration_id = 37
      `;
      assert.deepStrictEqual(migration, [{ migrationId: 37, name: "ProjectionThreadsPinned" }]);
    }),
  );
});
