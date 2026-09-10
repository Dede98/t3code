import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration075 from "./075_AgentControlVerificationCheckErrors.ts";

it.effect("upgrades verification error storage atomically without weakening evidence guards", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 74 });
    const before = yield* sql<{ type: string; name: string; sql: string | null }>`
      SELECT type, name, sql FROM sqlite_schema ORDER BY type, name
    `;
    const failed = yield* Effect.exit(
      sql.withTransaction(
        Effect.gen(function* () {
          yield* Migration075;
          yield* sql.unsafe("CREATE TABLE broken_check_upgrade(").unprepared;
        }),
      ),
    );
    assert.isTrue(Exit.isFailure(failed));
    assert.deepStrictEqual(
      yield* sql`SELECT type, name, sql FROM sqlite_schema ORDER BY type, name`,
      before,
    );
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 75 }), [
      [75, "AgentControlVerificationCheckErrors"],
    ]);
    const after = yield* sql<{ type: string; name: string; sql: string | null }>`
      SELECT type, name, sql FROM sqlite_schema ORDER BY type, name
    `;
    assert.equal(after.length, before.length);
    let extendedTables = 0;
    for (const entry of before) {
      const updated = after.find(
        (candidate) => candidate.type === entry.type && candidate.name === entry.name,
      );
      if (entry.type === "table" && entry.sql?.includes("'schema-violation'")) {
        extendedTables += 1;
        assert.include(updated!.sql!, "'verification-checks-missing'");
        assert.include(updated!.sql!, "'verification-checks-unavailable'");
        assert.include(updated!.sql!, "'verification-checks-stale'");
        assert.include(updated!.sql!, "'verification-checks-failed'");
        assert.equal(
          updated!
            .sql!.replace(
              /,\s*'verification-checks-missing', 'verification-checks-unavailable',\s*'verification-checks-stale', 'verification-checks-failed'/,
              "",
            )
            .replace(`CREATE TABLE "${entry.name}"`, `CREATE TABLE ${entry.name}`),
          entry.sql.replace(`CREATE TABLE "${entry.name}"`, `CREATE TABLE ${entry.name}`),
        );
      } else {
        assert.deepStrictEqual(updated, entry);
      }
    }
    assert.equal(extendedTables, 4);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 75 }), []);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
