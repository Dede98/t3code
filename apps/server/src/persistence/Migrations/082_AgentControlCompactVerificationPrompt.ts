import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT_V2,
  AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT,
} from "../../agentControl/verificationTurn/prompt.ts";

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const versions = "('agent-control-verification-prompt-v2', 'agent-control-verification-prompt-v3')";
const tables = [
  "agent_control_verification_handoff_intents",
  "agent_control_verification_evaluation_evidence",
] as const;
const triggers = [
  ["agent_control_verification_handoff_intent_validate", "NEW", true],
  ["agent_control_verification_handoff_result_contract_storage_validate", "NEW", true],
  ["agent_control_verification_handoff_result_contract_update_storage_validate", "NEW", true],
  ["agent_control_verification_result_source_seal_validate", "intent", true],
  ["agent_control_verification_result_capture_validate", "intent", false],
] as const;
const indexName = "idx_agent_control_verification_evaluation_candidate";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const schema = yield* sql<{ type: string; name: string; tbl_name: string; sql: string }>`
    SELECT type, name, tbl_name, sql FROM main.sqlite_schema
    WHERE sql IS NOT NULL ORDER BY rowid
  `;
  const replacements = new Map<string, string>();
  const extendVersion = (source: string, row = "") => {
    const pattern = new RegExp(
      `${row === "" ? "" : `${row}\\.`}prompt_template_version\\s*=\\s*'agent-control-verification-prompt-v2'`,
      "g",
    );
    if ([...source.matchAll(pattern)].length !== 1) {
      throw new Error("Compact verification prompt found a divergent version guard.");
    }
    return source.replace(
      pattern,
      `${row === "" ? "" : `${row}.`}prompt_template_version IN ${versions}`,
    );
  };
  for (const name of [...tables, indexName]) {
    const entry = schema.find((entry) => entry.name === name);
    if (entry === undefined || entry.type !== (name === indexName ? "index" : "table")) {
      return yield* Effect.die(
        new Error("Compact verification prompt found a missing schema object."),
      );
    }
    replacements.set(name, extendVersion(entry.sql));
  }
  for (const [name, row, hasFingerprint] of triggers) {
    const entry = schema.find((entry) => entry.name === name && entry.type === "trigger");
    if (entry === undefined) {
      return yield* Effect.die(
        new Error("Compact verification prompt found a missing authority guard."),
      );
    }
    let source = extendVersion(entry.sql, row);
    if (hasFingerprint) {
      const previous = `'${AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT_V2}'`;
      if (source.split(previous).length !== 2) {
        return yield* Effect.die(
          new Error("Compact verification prompt found a divergent fingerprint guard."),
        );
      }
      source = source.replace(
        previous,
        `CASE ${row}.prompt_template_version
        WHEN 'agent-control-verification-prompt-v2' THEN ${previous}
        WHEN 'agent-control-verification-prompt-v3' THEN '${AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT}' END`,
      );
    }
    replacements.set(name, source);
  }
  // Rebuild only CHECK constraints. Copy every stored byte, including legacy
  // prompts, fingerprints and event templates; replay retains its original contract.
  yield* sql`PRAGMA defer_foreign_keys = ON`;
  for (const entry of schema.filter((entry) => entry.type === "trigger" || entry.type === "view")) {
    yield* sql.unsafe(`DROP ${entry.type.toUpperCase()} main.${quote(entry.name)}`).unprepared;
  }
  for (const name of tables) {
    const temporaryName = `${name}_prompt_082`;
    yield* sql.unsafe(replacements.get(name)!.replace(name, temporaryName)).unprepared;
    yield* sql.unsafe(`INSERT INTO ${quote(temporaryName)} SELECT * FROM ${quote(name)}`)
      .unprepared;
    yield* sql.unsafe(`DROP TABLE ${quote(name)}`).unprepared;
    yield* sql.unsafe(`ALTER TABLE ${quote(temporaryName)} RENAME TO ${quote(name)}`).unprepared;
  }
  for (const entry of schema) {
    if (
      entry.type === "trigger" ||
      entry.type === "view" ||
      (entry.type === "index" && tables.some((name) => name === entry.tbl_name))
    ) {
      yield* sql.unsafe(replacements.get(entry.name) ?? entry.sql).unprepared;
    }
  }
  const violations = yield* sql`PRAGMA foreign_key_check`;
  if (violations.length !== 0) {
    return yield* Effect.die(
      new Error("Compact verification prompt introduced foreign-key violations."),
    );
  }
  yield* sql`PRAGMA defer_foreign_keys = OFF`;
});
