import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("039_ProjectionThreadsPinOrderKey", (it) => {
  it.effect("appends pin ordering after the fork keyset index", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 38 });
      const columnsBefore = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_threads)
      `;
      assert.ok(!columnsBefore.some((column) => column.name === "pin_order_key"));

      yield* runMigrations({ toMigrationInclusive: 39 });
      const columnsAfter = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_threads)
      `;
      assert.ok(columnsAfter.some((column) => column.name === "pin_order_key"));

      const migration = yield* sql<{
        readonly migrationId: number;
        readonly name: string;
      }>`
        SELECT migration_id AS "migrationId", name
        FROM effect_sql_migrations
        WHERE migration_id = 39
      `;
      assert.deepStrictEqual(migration, [
        { migrationId: 39, name: "ProjectionThreadsPinOrderKey" },
      ]);
    }),
  );
});
