import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const legacyAuthority = (alias: string) => `EXISTS (
  SELECT 1 FROM main.agent_control_verification_legacy_evaluations legacy
  JOIN main.agent_control_verification_evaluation_evidence evaluation
    ON evaluation.evaluation_id = legacy.evaluation_id
    AND evaluation.provider_delivery_id = legacy.provider_delivery_id
    AND evaluation.authority_digest = legacy.authority_digest
  WHERE legacy.provider_delivery_id = ${alias}.provider_delivery_id
    AND legacy.evaluation_id = ${alias}.evaluation_id
) AND ${alias}.delivery_terminal_state = 'completed'
  AND ${alias}.evaluation_authority = 'accepted-evaluation'
  AND ${alias}.evaluation_disposition = 'evaluated'
  AND ${alias}.verification_verdict IN ('passed', 'failed')`;

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE agent_control_verification_legacy_tasks (
    handoff_id TEXT PRIMARY KEY,
    task_finalization_evidence_id TEXT NOT NULL,
    finalization_fingerprint TEXT NOT NULL
  )`;
  yield* sql`INSERT INTO agent_control_verification_legacy_tasks
    SELECT task.handoff_id, task.task_finalization_evidence_id, task.finalization_fingerprint
    FROM main.agent_control_task_verification_finalization_evidence task
    JOIN main.agent_control_task_verification_finalization_receipts receipt
      ON receipt.receipt_id = task.receipt_id AND receipt.task_finalization_evidence_id = task.task_finalization_evidence_id
    JOIN main.agent_control_task_verification_finalization_markers marker
      ON marker.marker_id = task.marker_id AND marker.task_finalization_evidence_id = task.task_finalization_evidence_id
    JOIN main.agent_control_task_verification_finalization_publications publication
      ON publication.handoff_id = task.handoff_id AND publication.task_finalization_evidence_id = task.task_finalization_evidence_id
    JOIN main.agent_control_verification_finalization_evidence verification
      ON verification.finalization_evidence_id = task.verification_evidence_id
    JOIN main.agent_control_verification_legacy_evaluations legacy
      ON legacy.provider_delivery_id = verification.provider_delivery_id
      AND legacy.evaluation_id = verification.evaluation_id`;
  for (const operation of ["INSERT", "UPDATE", "DELETE"]) {
    yield* sql.unsafe(`CREATE TRIGGER agent_control_verification_legacy_tasks_no_${operation.toLowerCase()}
      BEFORE ${operation} ON agent_control_verification_legacy_tasks
      BEGIN SELECT RAISE(ABORT,'legacy task authority is fixed at upgrade'); END`).unprepared;
  }
  for (const [triggerName, source, target, outcome, cause, disposition, verdict, code] of [
    [
      "agent_control_task_verification_finalization_event_validate",
      "evidence",
      "json",
      "json_extract(NEW.payload_json, '$.verificationOutcome')",
      "json_extract(NEW.payload_json, '$.terminalCause')",
      "json_extract(NEW.payload_json, '$.evaluation.evaluationDisposition')",
      "json_extract(NEW.payload_json, '$.evaluation.verificationVerdict')",
      "json_extract(NEW.payload_json, '$.evaluation.invalidOutputCode')",
    ],
    [
      "agent_control_task_verification_finalization_evidence_validate",
      "verification",
      "row",
      "NEW.verification_outcome",
      "NEW.terminal_cause",
      "NEW.evaluation_disposition",
      "NEW.verification_verdict",
      "NEW.invalid_output_code",
    ],
  ] as const) {
    const rows = yield* sql<{
      sql: string;
    }>`SELECT sql FROM main.sqlite_schema WHERE type='trigger' AND name=${triggerName}`;
    const original = rows[0]?.sql;
    const outcomeGuard = `${source}.outcome = ${outcome}`;
    const newGuard = `((${outcomeGuard} AND ${source}.terminal_cause = ${cause}
      AND ${source}.evaluation_disposition IS ${disposition}
      AND ${source}.verification_verdict IS ${verdict}
      AND ${source}.invalid_output_code IS ${code}) OR (
        ${legacyAuthority(source)} AND ${outcome} = 'failed'
        AND ${cause} = 'verification-invalid-output' AND ${disposition} = 'invalid-output'
        AND ${verdict} IS NULL AND ${code} = 'verification-checks-missing'
      ))`;
    if (original === undefined || original.split(outcomeGuard).length !== 2)
      return yield* Effect.die(new Error("Legacy task finalization guard diverged"));
    let updated = original;
    for (const [field, value] of [
      ["terminal_cause", cause],
      ["evaluation_disposition", disposition],
      ["verification_verdict", verdict],
      ["invalid_output_code", code],
    ] as const) {
      const comparison = field === "terminal_cause" ? "=" : "IS";
      const expression = `${source}.${field} ${comparison}${target === "json" && field !== "terminal_cause" ? "\n              " : " "}${value}`;
      if (updated.split(expression).length !== 2)
        return yield* Effect.die(new Error(`Legacy task ${field} guard diverged`));
      updated = updated.replace(`AND ${expression}`, "");
    }
    updated = updated.replace(outcomeGuard, newGuard);
    yield* sql.unsafe(`DROP TRIGGER ${triggerName}`).unprepared;
    yield* sql.unsafe(updated).unprepared;
  }
});
