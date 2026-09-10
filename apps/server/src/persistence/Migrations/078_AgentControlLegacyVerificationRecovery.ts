import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const triggerName = "agent_control_verification_finalization_evidence_validate";
const previousGuard = `evaluation.disposition = NEW.evaluation_disposition
            AND evaluation.verdict IS NEW.verification_verdict
            AND evaluation.error_code IS NEW.invalid_output_code`;
const stageTriggerName = "agent_control_verification_terminal_stage_event_validate";
const previousStageGuard = `evidence.disposition = json_extract(NEW.payload_json, '$.evaluation.evaluationDisposition')
            AND evidence.verdict IS json_extract(NEW.payload_json, '$.evaluation.verificationVerdict')
            AND evidence.error_code IS json_extract(NEW.payload_json, '$.evaluation.invalidOutputCode')`;

export const captureLegacyVerificationEvaluations = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Freeze the upgrade boundary. Later missing evidence can never opt into legacy replay.
  yield* sql`CREATE TABLE agent_control_verification_legacy_evaluations (
    provider_delivery_id TEXT PRIMARY KEY,
    evaluation_id TEXT NOT NULL UNIQUE REFERENCES agent_control_verification_evaluation_evidence(evaluation_id),
    authority_digest TEXT NOT NULL
  )`;
  yield* sql`INSERT INTO agent_control_verification_legacy_evaluations
    SELECT provider_delivery_id,evaluation_id,authority_digest
    FROM agent_control_verification_evaluation_evidence
    WHERE json_valid(authority_json)
      AND json_type(authority_json, '$.verificationChecksDigest') IS NULL`;
  for (const operation of ["INSERT", "UPDATE", "DELETE"] as const) {
    yield* sql.unsafe(`CREATE TRIGGER agent_control_verification_legacy_evaluations_no_${operation.toLowerCase()}
      BEFORE ${operation} ON agent_control_verification_legacy_evaluations
      BEGIN SELECT RAISE(ABORT, 'legacy verification upgrade boundary is immutable'); END`)
      .unprepared;
  }
});

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const triggers = yield* sql<{ sql: string }>`SELECT sql FROM sqlite_schema
    WHERE type = 'trigger' AND name = ${triggerName}`;
  const trigger = triggers[0]?.sql;
  if (trigger === undefined || trigger.split(previousGuard).length !== 2) {
    return yield* Effect.die(
      new Error("Legacy verification found a divergent finalization guard."),
    );
  }
  const stageTriggers = yield* sql<{ sql: string }>`SELECT sql FROM sqlite_schema
    WHERE type = 'trigger' AND name = ${stageTriggerName}`;
  const stageTrigger = stageTriggers[0]?.sql;
  if (stageTrigger === undefined || stageTrigger.split(previousStageGuard).length !== 2) {
    return yield* Effect.die(new Error("Legacy verification found a divergent stage event guard."));
  }
  yield* captureLegacyVerificationEvaluations;
  const guard = `((${previousGuard}) OR (
    NEW.evaluation_disposition = 'invalid-output'
    AND NEW.verification_verdict IS NULL
    AND NEW.invalid_output_code = 'verification-checks-missing'
    AND EXISTS (
      SELECT 1 FROM agent_control_verification_legacy_evaluations legacy
      WHERE legacy.provider_delivery_id = NEW.provider_delivery_id
        AND legacy.evaluation_id = evaluation.evaluation_id
        AND legacy.authority_digest = evaluation.authority_digest
    )
  ))`;
  yield* sql.unsafe(`DROP TRIGGER ${triggerName}`).unprepared;
  yield* sql.unsafe(trigger.replace(previousGuard, guard)).unprepared;
  const stageGuard = `((${previousStageGuard}) OR (
    NEW.event_type = 'agentControl.stageRun.verificationFailed'
    AND json_extract(NEW.payload_json, '$.evaluation.evaluationDisposition') = 'invalid-output'
    AND json_extract(NEW.payload_json, '$.evaluation.verificationVerdict') IS NULL
    AND evidence.provider_delivery_id = json_extract(NEW.payload_json, '$.providerDeliveryId')
    AND evidence.provider_turn_id = json_extract(NEW.payload_json, '$.providerTurnId')
    AND evidence.handoff_id = json_extract(NEW.payload_json, '$.handoffId')
    AND (
      (json_extract(NEW.payload_json, '$.evaluation.invalidOutputCode') = 'verification-checks-missing'
        AND EXISTS (
          SELECT 1 FROM agent_control_verification_legacy_evaluations legacy
          WHERE legacy.provider_delivery_id = evidence.provider_delivery_id
            AND legacy.evaluation_id = evidence.evaluation_id
            AND legacy.authority_digest = evidence.authority_digest
        ))
      OR (json_extract(NEW.payload_json, '$.evaluation.invalidOutputCode') = 'verification-checks-stale'
        AND evidence.disposition = 'evaluated' AND evidence.verdict = 'passed'
        AND evidence.error_code IS NULL
        AND EXISTS (
          SELECT 1 FROM agent_control_verification_check_invalidations invalidation
          WHERE invalidation.provider_delivery_id = evidence.provider_delivery_id
            AND invalidation.provider_turn_id = evidence.provider_turn_id
            AND invalidation.handoff_id = evidence.handoff_id
        ))
    )
  ))`;
  yield* sql.unsafe(`DROP TRIGGER ${stageTriggerName}`).unprepared;
  yield* sql.unsafe(stageTrigger.replace(previousStageGuard, stageGuard)).unprepared;
});
