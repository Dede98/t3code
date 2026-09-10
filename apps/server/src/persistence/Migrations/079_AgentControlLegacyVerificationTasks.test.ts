import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import migration from "./079_AgentControlLegacyVerificationTasks.ts";

it.effect(
  "installs legacy task recovery atomically and fixes the replay whitelist at upgrade",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 78 });
      const before = yield* sql`SELECT name,sql FROM sqlite_schema ORDER BY name`;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* migration;
                return yield* Effect.fail("rollback");
              }),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(yield* sql`SELECT name,sql FROM sqlite_schema ORDER BY name`, before);
      yield* runMigrations({ toMigrationInclusive: 79 });
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO agent_control_verification_legacy_tasks VALUES ('handoff','evidence','fingerprint')`,
          ),
        ),
      );
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
