import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const previousCodes = "'malformed-json', 'unsupported-schema-version', 'schema-violation'";
const extendedCodes = `${previousCodes},
          'verification-checks-missing', 'verification-checks-unavailable',
          'verification-checks-stale', 'verification-checks-failed'`;
const tableNames = [
  "agent_control_verification_evaluation_evidence",
  "agent_control_verification_evaluation_receipts",
  "agent_control_verification_finalization_evidence",
  "agent_control_task_verification_finalization_evidence",
] as const;

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const schema = yield* sql<{ type: string; name: string; tbl_name: string; sql: string }>`
    SELECT type, name, tbl_name, sql FROM main.sqlite_schema
    WHERE sql IS NOT NULL ORDER BY rowid
  `;
  const replacements = new Map<string, string>();
  for (const tableName of tableNames) {
    const table = schema.find((entry) => entry.type === "table" && entry.name === tableName);
    if (table === undefined || table.sql.split(previousCodes).length !== 2) {
      return yield* Effect.die(new Error("Verification check errors found a divergent table."));
    }
    replacements.set(tableName, table.sql.replace(previousCodes, extendedCodes));
  }
  yield* sql`PRAGMA defer_foreign_keys = ON`;
  // SQLite validates dependent views/triggers during renames. Restore their
  // exact definitions so evidence immutability and authority guards survive.
  for (const entry of schema.filter((entry) => entry.type === "trigger" || entry.type === "view")) {
    yield* sql.unsafe(`DROP ${entry.type.toUpperCase()} main.${quote(entry.name)}`).unprepared;
  }
  for (const [tableName, source] of replacements) {
    const temporaryName = `${tableName}_checks_075`;
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
      yield* sql.unsafe(entry.sql).unprepared;
    }
  }
  const violations = yield* sql`PRAGMA foreign_key_check`;
  if (violations.length !== 0) {
    return yield* Effect.die(
      new Error("Verification check errors introduced foreign-key violations."),
    );
  }
  // Rebuilt parents satisfy the checked FKs; clear SQLite's deferred DROP debt.
  yield* sql`PRAGMA defer_foreign_keys = OFF`;
});
