import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const budgets = [
  ["agent_control_implementation_handoff_intents", 120000, 186560],
  ["agent_control_verification_handoff_intents", 1048576, 4194304],
] as const;
const verificationTriggerName = "agent_control_verification_handoff_intent_validate";
const previousVerificationBudget = "length(CAST(NEW.prompt_text AS BLOB)) <= 1048576";
const verificationBudget = `length(CAST(NEW.prompt_text AS BLOB)) <= CASE WHEN EXISTS (
          SELECT 1 FROM agent_control_stage_run_states stage
          WHERE stage.stage_run_id = materialization.stage_run_id
            AND stage.stage_kind = 'verification' AND stage.stage_ordinal = 5
        ) AND EXISTS (
          SELECT 1 FROM agent_control_run_once_repairs repair
          WHERE repair.repair_stage_run_id = admission.implementation_stage_run_id
        ) THEN 4194304 ELSE 1048576 END`;

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const schema = yield* sql<{ type: string; name: string; tbl_name: string; sql: string }>`
    SELECT type, name, tbl_name, sql FROM main.sqlite_schema
    WHERE sql IS NOT NULL ORDER BY rowid
  `;
  const replacements = new Map<string, string>();
  for (const [tableName, previous, next] of budgets) {
    const table = schema.find((entry) => entry.type === "table" && entry.name === tableName);
    const previousBudget = `length(CAST(prompt_text AS BLOB)) BETWEEN 1 AND ${previous}`;
    if (table === undefined || table.sql.split(previousBudget).length !== 2) {
      return yield* Effect.die(new Error("Repair prompt budget found a divergent handoff table."));
    }
    replacements.set(
      tableName,
      table.sql.replace(previousBudget, `length(CAST(prompt_text AS BLOB)) BETWEEN 1 AND ${next}`),
    );
  }
  const verificationTrigger = schema.find((entry) => entry.name === verificationTriggerName);
  if (
    verificationTrigger === undefined ||
    verificationTrigger.sql.split(previousVerificationBudget).length !== 2
  ) {
    return yield* Effect.die(
      new Error("Repair prompt budget found a divergent verification guard."),
    );
  }
  // Retain the complete 120KB implementation input, bounded 64KiB verdict and
  // 1KiB framing. Re-verification also embeds its repaired delivery evidence.
  // Applied 073 databases need this separate upgrade on restart.
  yield* sql`PRAGMA defer_foreign_keys = ON`;
  // Dependent views/triggers must be absent during SQLite's rename validation.
  // Restore their exact definitions, including all existing evidence guards.
  for (const entry of schema.filter((entry) => entry.type === "trigger" || entry.type === "view")) {
    yield* sql.unsafe(`DROP ${entry.type.toUpperCase()} main.${quote(entry.name)}`).unprepared;
  }
  for (const [tableName, source] of replacements) {
    const temporaryName = `${tableName}_budget_074`;
    yield* sql.unsafe(source.replace(tableName, temporaryName)).unprepared;
    yield* sql.unsafe(`INSERT INTO ${quote(temporaryName)} SELECT * FROM ${quote(tableName)}`)
      .unprepared;
    yield* sql.unsafe(`DROP TABLE ${quote(tableName)}`).unprepared;
    yield* sql.unsafe(`ALTER TABLE ${quote(temporaryName)} RENAME TO ${quote(tableName)}`)
      .unprepared;
  }
  for (const entry of schema) {
    if (
      entry.type === "trigger" ||
      entry.type === "view" ||
      (entry.type === "index" && replacements.has(entry.tbl_name))
    ) {
      const source =
        entry.name === verificationTriggerName
          ? entry.sql.replace(previousVerificationBudget, verificationBudget)
          : entry.sql;
      yield* sql.unsafe(source).unprepared;
    }
  }
  yield* sql`
    CREATE TRIGGER agent_control_implementation_prompt_budget_validate
    BEFORE INSERT ON agent_control_implementation_handoff_intents
    WHEN length(CAST(NEW.prompt_text AS BLOB)) > 120000
      AND NOT EXISTS (
        SELECT 1 FROM agent_control_run_once_repairs repair
        WHERE repair.repair_stage_run_id = NEW.stage_run_id
      )
    BEGIN SELECT RAISE(ABORT, 'implementation prompt exceeds its byte budget'); END
  `;
  const violations = yield* sql`PRAGMA foreign_key_check`;
  if (violations.length !== 0) {
    return yield* Effect.die(new Error("Repair prompt budget introduced foreign-key violations."));
  }
  // Clear the deferred DROP TABLE debt after checking the rebuilt parent.
  yield* sql`PRAGMA defer_foreign_keys = OFF`;
});
