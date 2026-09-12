import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration082 from "./082_AgentControlCompactVerificationPrompt.ts";

it.effect("preserves TEMP and attached shadows through upgrade, rollback and replay", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 81 });
    yield* sql`ATTACH DATABASE ':memory:' AS migration082_shadow`;
    const tableNames = [
      "agent_control_verification_handoff_intents",
      "agent_control_verification_evaluation_evidence",
    ].flatMap((name) => [name, `${name}_prompt_082`]);
    const [view] = yield* sql<{ name: string }>`
      SELECT name FROM main.sqlite_schema WHERE type = 'view' ORDER BY name LIMIT 1
    `;
    assert.isDefined(view);
    for (const schema of ["temp", "migration082_shadow"] as const) {
      for (const name of tableNames) {
        yield* sql.unsafe(`CREATE TABLE ${schema}.${name} (shadow_value TEXT)`).unprepared;
        yield* sql.unsafe(`INSERT INTO ${schema}.${name} VALUES ('preserve shadow')`).unprepared;
      }
      yield* sql.unsafe(`CREATE INDEX ${schema}.idx_agent_control_verification_evaluation_candidate
        ON agent_control_verification_evaluation_evidence(shadow_value)`).unprepared;
      yield* sql.unsafe(`CREATE TRIGGER ${schema}.agent_control_verification_handoff_intent_validate
        BEFORE INSERT ON agent_control_verification_handoff_intents BEGIN SELECT 1; END`)
        .unprepared;
      yield* sql.unsafe(`CREATE VIEW ${schema}."${view!.name}"
        AS SELECT shadow_value FROM agent_control_verification_handoff_intents`).unprepared;
    }
    const snapshotShadows = Effect.gen(function* () {
      const snapshots = [];
      for (const schema of ["temp", "migration082_shadow"] as const) {
        snapshots.push(
          yield* sql.unsafe(
            `SELECT type, name, sql FROM ${schema}.sqlite_schema ORDER BY type, name`,
          ),
        );
        for (const name of tableNames) {
          snapshots.push(yield* sql.unsafe(`SELECT * FROM ${schema}.${name}`));
        }
      }
      return snapshots;
    });
    const beforeShadows = yield* snapshotShadows;
    const beforeMain =
      yield* sql`SELECT type, name, sql FROM main.sqlite_schema ORDER BY type, name`;
    const failed = yield* Effect.exit(
      sql.withTransaction(
        Effect.gen(function* () {
          yield* Migration082;
          return yield* Effect.fail("rollback upgrade");
        }),
      ),
    );
    assert.deepStrictEqual(failed, Exit.fail("rollback upgrade"));
    assert.deepStrictEqual(yield* snapshotShadows, beforeShadows);
    assert.deepStrictEqual(
      yield* sql`SELECT type, name, sql FROM main.sqlite_schema ORDER BY type, name`,
      beforeMain,
    );
    assert.deepStrictEqual(yield* runMigrations(), [[82, "AgentControlCompactVerificationPrompt"]]);
    assert.deepStrictEqual(yield* snapshotShadows, beforeShadows);
    assert.deepStrictEqual(yield* sql`PRAGMA main.foreign_key_check`, []);
    const afterMain =
      yield* sql`SELECT type, name, sql FROM main.sqlite_schema ORDER BY type, name`;
    assert.deepStrictEqual(yield* runMigrations(), []);
    assert.deepStrictEqual(yield* snapshotShadows, beforeShadows);
    assert.deepStrictEqual(
      yield* sql`SELECT type, name, sql FROM main.sqlite_schema ORDER BY type, name`,
      afterMain,
    );
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect(
  "upgrades prompt contracts atomically while retaining every unrelated evidence guard",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 81 });
      const before = yield* sql<{ type: string; name: string; sql: string | null }>`
      SELECT type, name, sql FROM sqlite_schema ORDER BY type, name
    `;
      const failed = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* Migration082;
            return yield* Effect.fail("rollback upgrade");
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(failed));
      assert.deepStrictEqual(
        yield* sql`SELECT type, name, sql FROM sqlite_schema ORDER BY type, name`,
        before,
      );
      assert.deepStrictEqual(yield* runMigrations(), [
        [82, "AgentControlCompactVerificationPrompt"],
      ]);
      const after = yield* sql<{ type: string; name: string; sql: string | null }>`
      SELECT type, name, sql FROM sqlite_schema ORDER BY type, name
    `;
      assert.equal(after.length, before.length);
      const changed = after.filter((entry, index) => entry.sql !== before[index]!.sql);
      assert.deepStrictEqual(
        changed.map((entry) => entry.name),
        [
          "idx_agent_control_verification_evaluation_candidate",
          "agent_control_verification_evaluation_evidence",
          "agent_control_verification_handoff_intents",
          "agent_control_verification_handoff_intent_validate",
          "agent_control_verification_handoff_result_contract_storage_validate",
          "agent_control_verification_handoff_result_contract_update_storage_validate",
          "agent_control_verification_result_capture_validate",
          "agent_control_verification_result_source_seal_validate",
        ],
      );
      // Collapse only the added version alternatives and fingerprint CASE. Every
      // other byte of the authority and immutability guards must remain identical.
      for (const entry of changed) {
        const previous = before.find((candidate) => candidate.name === entry.name)!;
        const normalized = entry
          .sql!.replaceAll(
            "prompt_template_version IN ('agent-control-verification-prompt-v2', 'agent-control-verification-prompt-v3')",
            "prompt_template_version = 'agent-control-verification-prompt-v2'",
          )
          .replace(
            /CASE (?:NEW|intent)\.prompt_template_version\s+WHEN 'agent-control-verification-prompt-v2' THEN ('[a-f0-9]{64}')\s+WHEN 'agent-control-verification-prompt-v3' THEN '[a-f0-9]{64}' END/g,
            "$1",
          );
        const normalizeWhitespace = (source: string) =>
          source
            .replace(/\s+/g, " ")
            .replace(`CREATE TABLE "${entry.name}"`, `CREATE TABLE ${entry.name}`);
        assert.equal(normalizeWhitespace(normalized), normalizeWhitespace(previous.sql!));
      }
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
