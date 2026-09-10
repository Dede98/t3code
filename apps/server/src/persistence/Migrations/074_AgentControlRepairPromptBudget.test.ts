import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration074 from "./074_AgentControlRepairPromptBudget.ts";

it.effect("upgrades an applied repair migration atomically and preserves its evidence guards", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 73 });
    const before = yield* sql<{ type: string; name: string; sql: string | null }>`
      SELECT type, name, sql FROM sqlite_schema ORDER BY type, name
    `;
    const failed = yield* Effect.exit(
      sql.withTransaction(
        Effect.gen(function* () {
          yield* Migration074;
          yield* sql.unsafe("CREATE TABLE broken_budget_upgrade(").unprepared;
        }),
      ),
    );
    assert.isTrue(Exit.isFailure(failed));
    assert.deepStrictEqual(
      yield* sql`SELECT type, name, sql FROM sqlite_schema ORDER BY type, name`,
      before,
    );
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 74 }), [
      [74, "AgentControlRepairPromptBudget"],
    ]);
    const after = yield* sql<{ type: string; name: string; sql: string | null }>`
      SELECT type, name, sql FROM sqlite_schema ORDER BY type, name
    `;
    for (const entry of before.filter(
      (entry) => entry.type === "trigger" || entry.type === "view",
    )) {
      if (entry.name === "agent_control_verification_handoff_intent_validate") {
        const updated = after.find((candidate) => candidate.name === entry.name)?.sql;
        assert.isString(updated);
        assert.deepStrictEqual(
          updated?.replace(
            /length\(CAST\(NEW.prompt_text AS BLOB\)\) <= CASE WHEN EXISTS \([\s\S]+?THEN 4194304 ELSE 1048576 END/,
            "length(CAST(NEW.prompt_text AS BLOB)) <= 1048576",
          ),
          entry.sql,
        );
        assert.include(updated!, "stage.stage_ordinal = 5");
        assert.include(
          updated!,
          "repair.repair_stage_run_id = admission.implementation_stage_run_id",
        );
        continue;
      }
      assert.deepStrictEqual(
        after.find((candidate) => candidate.type === entry.type && candidate.name === entry.name),
        entry,
      );
    }
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 74 }), []);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
