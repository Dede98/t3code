import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration077 from "./077_AgentControlVerificationCheckInvalidations.ts";

it.effect("atomically installs a narrow immutable proof for late code changes", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 76 });
    const before = yield* sql`SELECT type,name,sql FROM sqlite_schema ORDER BY type,name`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* Migration077;
              yield* sql.unsafe("CREATE TABLE broken_invalidation(").unprepared;
            }),
          ),
        ),
      ),
    );
    assert.deepStrictEqual(
      yield* sql`SELECT type,name,sql FROM sqlite_schema ORDER BY type,name`,
      before,
    );
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 77 }), [
      [77, "AgentControlVerificationCheckInvalidations"],
    ]);
    yield* sql`INSERT INTO agent_control_verification_check_manifests VALUES
      ('delivery','handoff',3,'/fixture','checked-code','[]','manifest','2026-09-10T00:00:00Z')`;
    yield* sql`INSERT INTO agent_control_verification_check_assessments VALUES
      ('delivery','turn',NULL,'assessment','2026-09-10T00:00:00Z')`;
    for (const [delivery, turn, handoff, assessment, observed] of [
      ["foreign-delivery", "turn", "handoff", "assessment", "changed-code"],
      ["delivery", "foreign-turn", "handoff", "assessment", "changed-code"],
      ["delivery", "turn", "foreign-handoff", "assessment", "changed-code"],
      ["delivery", "turn", "handoff", "foreign-assessment", "changed-code"],
      ["delivery", "turn", "handoff", "assessment", "checked-code"],
    ]) {
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`INSERT INTO agent_control_verification_check_invalidations VALUES
        (${delivery},${turn},${handoff},${assessment},${observed},'2026-09-10T00:00:01Z')`),
        ),
      );
    }
    yield* sql`INSERT INTO agent_control_verification_check_invalidations VALUES
      ('delivery','turn','handoff','assessment','changed-code','2026-09-10T00:00:01Z')`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`UPDATE agent_control_verification_check_invalidations SET observed_code_digest='forged'`,
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(sql`DELETE FROM agent_control_verification_check_invalidations`),
      ),
    );
    assert.deepStrictEqual(
      yield* sql`SELECT observed_code_digest AS code FROM agent_control_verification_check_invalidations`,
      [{ code: "changed-code" }],
    );
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 77 }), []);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
