import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("038_ProjectionTurnsKeysetIndex", (it) => {
  it.effect("appends the pagination index after the fork pin migration", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 37 });
      const indexesBefore = yield* sql<{ readonly name: string }>`
        PRAGMA index_list(projection_turns)
      `;
      assert.ok(
        !indexesBefore.some((index) => index.name === "idx_projection_turns_thread_keyset"),
      );

      yield* runMigrations({ toMigrationInclusive: 38 });
      const indexColumns = yield* sql<{ readonly name: string }>`
        PRAGMA index_info(idx_projection_turns_thread_keyset)
      `;
      assert.deepStrictEqual(
        indexColumns.map((column) => column.name),
        ["thread_id", "requested_at", "turn_id"],
      );

      const migration = yield* sql<{
        readonly migrationId: number;
        readonly name: string;
      }>`
        SELECT migration_id AS "migrationId", name
        FROM effect_sql_migrations
        WHERE migration_id = 38
      `;
      assert.deepStrictEqual(migration, [{ migrationId: 38, name: "ProjectionTurnsKeysetIndex" }]);
    }),
  );
});
