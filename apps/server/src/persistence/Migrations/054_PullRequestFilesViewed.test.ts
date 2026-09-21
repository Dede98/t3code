import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "../Migrations.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("054_PullRequestFilesViewed", (it) => {
  it.effect("adds viewed-file storage without changing the existing fork migration history", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 53 });
      const previousHistory = yield* sql`
        SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
      `;

      yield* runMigrations();
      yield* sql`
        INSERT INTO pull_request_files_viewed
          (provider, host, repository, number, viewer, path, revision, viewed_at)
        VALUES ('gitlab', 'gitlab.com', 'group/repo', 1, 'reader', 'file.ts', 'revision-1', '2026-09-18')
      `;
      yield* runMigrations();

      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM effect_sql_migrations
          WHERE migration_id <= 53 ORDER BY migration_id`,
        previousHistory,
      );
      assert.deepEqual(yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = 54`, [
        { name: "PullRequestFilesViewed" },
      ]);
      assert.deepEqual(yield* sql`SELECT path, revision FROM pull_request_files_viewed`, [
        { path: "file.ts", revision: "revision-1" },
      ]);
    }),
  );
});
