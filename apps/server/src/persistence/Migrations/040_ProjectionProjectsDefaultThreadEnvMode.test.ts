import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("040_ProjectionProjectsDefaultThreadEnvMode", (it) => {
  it.effect("adds the nullable default thread environment mode to project projections", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 39 });
      yield* runMigrations({ toMigrationInclusive: 40 });

      const columns = yield* sql<{ readonly name: string; readonly notnull: number }>`
        PRAGMA table_info(projection_projects)
      `;
      const defaultThreadEnvMode = columns.find(
        (column) => column.name === "default_thread_env_mode",
      );

      assert.equal(defaultThreadEnvMode?.name, "default_thread_env_mode");
      assert.equal(defaultThreadEnvMode?.notnull, 0);
    }),
  );
});
