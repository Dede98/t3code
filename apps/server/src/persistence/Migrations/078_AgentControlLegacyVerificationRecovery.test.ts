import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration078, {
  captureLegacyVerificationEvaluations,
} from "./078_AgentControlLegacyVerificationRecovery.ts";

it.effect("captures only pre-upgrade authority and prevents later opt-in to legacy replay", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE agent_control_verification_evaluation_evidence AS
      SELECT 'old-evaluation' AS evaluation_id, 'old-delivery' AS provider_delivery_id,
        'old-digest' AS authority_digest, '{"verdict":"passed"}' AS authority_json
      UNION ALL SELECT 'current-evaluation','current-delivery','current-digest','{"verificationChecksDigest":"checks"}'
      UNION ALL SELECT 'broken-evaluation','broken-delivery','broken-digest','invalid JSON'`;
    yield* sql`CREATE UNIQUE INDEX evaluation_identity ON agent_control_verification_evaluation_evidence(evaluation_id)`;
    yield* captureLegacyVerificationEvaluations;
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM agent_control_verification_legacy_evaluations`,
      [
        {
          provider_delivery_id: "old-delivery",
          evaluation_id: "old-evaluation",
          authority_digest: "old-digest",
        },
      ],
    );
    for (const mutation of [
      sql`INSERT INTO agent_control_verification_legacy_evaluations VALUES ('current-delivery','current-evaluation','current-digest')`,
      sql`UPDATE agent_control_verification_legacy_evaluations SET authority_digest='forged'`,
      sql`DELETE FROM agent_control_verification_legacy_evaluations`,
    ])
      assert.isTrue(Exit.isFailure(yield* Effect.exit(mutation)));
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect("installs legacy recovery atomically while retaining the late-code-change guard", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 77 });
    const before = yield* sql`SELECT type,name,sql FROM sqlite_schema ORDER BY type,name`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* Migration078;
              yield* sql.unsafe("CREATE TABLE broken_legacy_recovery(").unprepared;
            }),
          ),
        ),
      ),
    );
    assert.deepStrictEqual(
      yield* sql`SELECT type,name,sql FROM sqlite_schema ORDER BY type,name`,
      before,
    );
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 78 }), [
      [78, "AgentControlLegacyVerificationRecovery"],
    ]);
    const triggers = yield* sql<{ sql: string }>`SELECT sql FROM sqlite_schema
      WHERE type='trigger' AND name='agent_control_verification_finalization_evidence_validate'`;
    assert.include(triggers[0]!.sql, "agent_control_verification_check_invalidations");
    assert.include(triggers[0]!.sql, "agent_control_verification_legacy_evaluations");
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 78 }), []);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
