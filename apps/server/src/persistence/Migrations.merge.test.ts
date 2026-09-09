import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "./NodeSqliteClient.ts";
import {
  makeMigrationLoader,
  makeAgentControlMigrationLoader,
  runMigrations,
} from "./Migrations.ts";

for (const history of ["main", "t3auto"] as const) {
  it.effect(
    `upgrades an existing ${history} database without losing either migration history`,
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const migrate = Migrator.make({});
        yield* migrate({ loader: makeMigrationLoader(history === "main" ? undefined : 33) });
        if (history === "t3auto") yield* migrate({ loader: makeAgentControlMigrationLoader(65) });
        yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
        VALUES ('existing', 'Keep me', '/existing', '[]', '2026-07-01T00:00:00.000Z', '2026-07-01T00:00:00.000Z')`;
        const oldJournal =
          yield* sql`SELECT migration_id, name, created_at FROM main.effect_sql_migrations ORDER BY migration_id`;
        yield* runMigrations();
        assert.deepStrictEqual(
          yield* sql`SELECT title FROM projection_projects WHERE project_id = 'existing'`,
          [{ title: "Keep me" }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT name FROM main.effect_sql_migrations WHERE migration_id = 34`,
          [{ name: "ProjectionThreadsSettled" }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT name FROM main.effect_sql_agent_control_migrations WHERE migration_id = 34`,
          [{ name: "OrchestrationCommandAuthority" }],
        );
        const retained =
          history === "main"
            ? yield* sql`SELECT migration_id, name, created_at FROM main.effect_sql_migrations ORDER BY migration_id`
            : yield* sql`SELECT migration_id, name, created_at FROM main.effect_sql_migrations WHERE migration_id <= 33 UNION ALL SELECT migration_id, name, created_at FROM main.effect_sql_agent_control_migrations WHERE migration_id <= 65 ORDER BY migration_id`;
        assert.deepStrictEqual(retained, oldJournal);
        assert.deepStrictEqual(yield* runMigrations(), []);
        const columns = yield* sql<{ name: string }>`PRAGMA table_info(projection_threads)`;
        for (const name of [
          "agent_control_json",
          "settled_at",
          "active_order_key",
          "branch_pull_request_json",
        ]) {
          assert.isTrue(
            columns.some((column) => column.name === name),
            name,
          );
        }
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
}
