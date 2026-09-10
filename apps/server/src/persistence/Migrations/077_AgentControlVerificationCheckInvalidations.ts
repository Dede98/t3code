import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const triggerName = "agent_control_verification_finalization_evidence_validate";
const previousEvaluationGuard = `evaluation.disposition = NEW.evaluation_disposition
            AND evaluation.verdict IS NEW.verification_verdict
            AND evaluation.error_code IS NEW.invalid_output_code`;
const evaluationGuard = `((${previousEvaluationGuard}) OR (
              evaluation.disposition = 'evaluated' AND evaluation.verdict = 'passed'
              AND evaluation.error_code IS NULL
              AND NEW.evaluation_disposition = 'invalid-output'
              AND NEW.verification_verdict IS NULL
              AND NEW.invalid_output_code = 'verification-checks-stale'
              AND EXISTS (
                SELECT 1 FROM agent_control_verification_check_invalidations invalidation
                WHERE invalidation.provider_delivery_id = NEW.provider_delivery_id
                  AND invalidation.provider_turn_id = NEW.provider_turn_id
                  AND invalidation.handoff_id = NEW.handoff_id
              )
            ))`;

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const triggers = yield* sql<{ sql: string }>`SELECT sql FROM sqlite_schema
    WHERE type = 'trigger' AND name = ${triggerName}`;
  const trigger = triggers[0]?.sql;
  if (trigger === undefined || trigger.split(previousEvaluationGuard).length !== 2) {
    return yield* Effect.die(
      new Error("Verification invalidation found a divergent finalization guard."),
    );
  }
  // A late code change invalidates finalization without rewriting the accepted
  // evaluation. This durable proof permits only the existing failed/no-repair path.
  yield* sql`CREATE TABLE agent_control_verification_check_invalidations (
    provider_delivery_id TEXT PRIMARY KEY REFERENCES agent_control_verification_check_assessments(provider_delivery_id),
    provider_turn_id TEXT NOT NULL,
    handoff_id TEXT NOT NULL,
    assessment_digest TEXT NOT NULL,
    observed_code_digest TEXT NOT NULL,
    invalidated_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TRIGGER agent_control_verification_check_invalidation_validate
    BEFORE INSERT ON agent_control_verification_check_invalidations
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_verification_check_assessments assessment
      JOIN agent_control_verification_check_manifests manifest
        ON manifest.provider_delivery_id = assessment.provider_delivery_id
      WHERE assessment.provider_delivery_id = NEW.provider_delivery_id
        AND assessment.provider_turn_id = NEW.provider_turn_id
        AND assessment.digest = NEW.assessment_digest AND assessment.code IS NULL
        AND manifest.handoff_id = NEW.handoff_id
        AND manifest.code_digest != NEW.observed_code_digest
    )
    BEGIN SELECT RAISE(ABORT, 'verification invalidation lacks a changed checked code state'); END`;
  for (const operation of ["UPDATE", "DELETE"] as const) {
    yield* sql.unsafe(`CREATE TRIGGER agent_control_verification_check_invalidations_no_${operation.toLowerCase()}
      BEFORE ${operation} ON agent_control_verification_check_invalidations
      BEGIN SELECT RAISE(ABORT, 'verification invalidation is immutable'); END`).unprepared;
  }
  yield* sql.unsafe(`DROP TRIGGER ${triggerName}`).unprepared;
  yield* sql.unsafe(trigger.replace(previousEvaluationGuard, evaluationGuard)).unprepared;
});
