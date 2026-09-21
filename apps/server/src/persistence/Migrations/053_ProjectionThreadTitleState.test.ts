import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "../Migrations.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))(
  "053_ProjectionThreadTitleState",
  (it) => {
    it.effect(
      "upgrades the existing fork migration history without replacing message context",
      () =>
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* runMigrations({ toMigrationInclusive: 52 });
          yield* sql`
        INSERT INTO projection_projects
          (project_id, title, workspace_root, scripts_json, created_at, updated_at)
        VALUES ('project-1', 'Project', '/tmp/project', '[]', '2026-09-15', '2026-09-15')
      `;
          yield* sql`
        INSERT INTO projection_threads
          (thread_id, project_id, title, model_selection_json, created_at, updated_at)
        VALUES ('thread-1', 'project-1', 'Existing title',
          '{"instanceId":"codex","model":"gpt-5"}', '2026-09-15', '2026-09-15')
      `;
          const previousHistory = yield* sql`
        SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
      `;

          yield* runMigrations({ toMigrationInclusive: 53 });
          yield* runMigrations({ toMigrationInclusive: 53 });

          assert.deepEqual(
            yield* sql`SELECT migration_id, name FROM effect_sql_migrations
          WHERE migration_id <= 52 ORDER BY migration_id`,
            previousHistory,
          );
          assert.deepEqual(
            yield* sql`SELECT title, title_state_json FROM projection_threads WHERE thread_id = 'thread-1'`,
            [{ title: "Existing title", title_state_json: null }],
          );
          const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_thread_messages)
      `;
          assert.isTrue(columns.some((column) => column.name === "context_json"));
          assert.deepEqual(
            yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = 53`,
            [{ name: "ProjectionThreadTitleState" }],
          );
        }),
    );
  },
);
