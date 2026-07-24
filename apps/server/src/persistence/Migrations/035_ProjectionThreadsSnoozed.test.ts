import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("035_ProjectionThreadsSnoozed", (it) => {
  it.effect("adds snoozed thread columns after the fork settled migration", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 34 });
      const columnsBefore = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_threads)
      `;
      assert.equal(
        columnsBefore.some((column) => column.name === "snoozed_until"),
        false,
      );
      assert.equal(
        columnsBefore.some((column) => column.name === "snoozed_at"),
        false,
      );

      yield* runMigrations({ toMigrationInclusive: 35 });
      const columnsAfter = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_threads)
      `;
      assert.equal(
        columnsAfter.some((column) => column.name === "snoozed_until"),
        true,
      );
      assert.equal(
        columnsAfter.some((column) => column.name === "snoozed_at"),
        true,
      );

      const migration = yield* sql<{
        readonly migrationId: number;
        readonly name: string;
      }>`
        SELECT migration_id AS "migrationId", name
        FROM effect_sql_migrations
        WHERE migration_id = 35
      `;
      assert.deepStrictEqual(migration, [{ migrationId: 35, name: "ProjectionThreadsSnoozed" }]);
    }),
  );
});
