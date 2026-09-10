import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { providerAdmissionDeliveryTables } from "../../agentControl/providerAdmission/preInvokeRecovery.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE agent_control_prepared_session_archive (
      stage TEXT NOT NULL CHECK (stage IN ('initial-planning', 'implementation', 'verification')),
      provider_delivery_id TEXT NOT NULL,
      evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
      delivery_json TEXT NOT NULL CHECK (json_valid(delivery_json)),
      reason TEXT NOT NULL CHECK (reason = 'session-prepared-before-delivery-cas'),
      PRIMARY KEY (stage, provider_delivery_id)
    )
  `;
  for (const operation of ["UPDATE", "DELETE"] as const) {
    yield* sql.unsafe(`CREATE TRIGGER agent_control_prepared_session_archive_no_${operation.toLowerCase()}
      BEFORE ${operation} ON agent_control_prepared_session_archive
      BEGIN SELECT RAISE(ABORT, 'prepared session archive is immutable'); END`).unprepared;
  }
  const fields = [
    "provider_delivery_id",
    "thread_id",
    "provider_instance_id",
    "runtime_mode",
    "cwd",
    "model_selection_json",
    "model_selection_fingerprint",
    "session_created_at",
    "resume_cursor_json",
    "recorded_at",
  ];
  for (const [stage, deliveries, attestations] of providerAdmissionDeliveryTables) {
    const sessions = deliveries.replace(/_deliveries$/, "_session_evidence");
    const eligible = `delivery.state IN ('claimed', 'retry-wait')
      AND delivery.provider_turn_id IS NULL AND delivery.provider_accepted_at IS NULL
      AND delivery.provider_session_created_at IS NULL
      AND delivery.provider_resume_cursor_json IS NULL AND delivery.terminal_at IS NULL
      AND NOT EXISTS (SELECT 1 FROM main.${attestations} attestation
        WHERE attestation.provider_delivery_id = delivery.provider_delivery_id)`;
    const rows = yield* sql.unsafe<{
      providerDeliveryId: string;
      state: string;
      revision: number;
      updatedAt: string;
      evidenceJson: string;
      deliveryJson: string;
    }>(`SELECT delivery.provider_delivery_id AS providerDeliveryId, delivery.state,
      delivery.revision, delivery.updated_at AS updatedAt,
      json_object(${fields.map((field) => `'${field}', session.${field}`).join(",")}) AS evidenceJson,
      json_object('state',delivery.state,'revision',delivery.revision,
        'claim_owner_id',delivery.claim_owner_id,'claim_generation',delivery.claim_generation,
        'claim_expires_at',delivery.claim_expires_at,'updated_at',delivery.updated_at) AS deliveryJson
      FROM main.${sessions} session JOIN main.${deliveries} delivery
        ON delivery.provider_delivery_id = session.provider_delivery_id
      WHERE ${eligible}`);
    if (rows.length === 0) continue;
    const triggerName = `${sessions}_no_delete`;
    const triggers = yield* sql<{ source: string }>`SELECT sql AS source FROM main.sqlite_schema
      WHERE type='trigger' AND name=${triggerName} AND tbl_name=${sessions}`;
    const source = triggers[0]?.source;
    const noun =
      stage === "initial-planning" ? "initial planning session evidence" : `${stage} evidence`;
    const expected = `CREATE TRIGGER ${triggerName} BEFORE DELETE ON ${sessions} BEGIN SELECT RAISE(ABORT, '${noun} is immutable'); END`;
    if (source === undefined || source.replace(/\s+/g, " ").trim() !== expected) {
      return yield* Effect.die(
        new Error("Prepared session recovery found a divergent immutability guard."),
      );
    }
    yield* sql.unsafe(`DROP TRIGGER main.${triggerName}`).unprepared;
    for (const row of rows) {
      yield* sql`INSERT INTO agent_control_prepared_session_archive
        (stage, provider_delivery_id, evidence_json, delivery_json, reason)
        VALUES (${stage}, ${row.providerDeliveryId}, ${row.evidenceJson}, ${row.deliveryJson},
          'session-prepared-before-delivery-cas')`;
      // Invalidate an old prepared worker's CAS before removing its session binding.
      // Accepted turns always have immutable attestation and are never selected above.
      if (row.state === "claimed") {
        const invalidated = yield* sql.unsafe(
          `UPDATE main.${deliveries}
          SET state='retry-wait', revision=revision+1, claim_owner_id=NULL, claim_expires_at=NULL,
            next_attempt_at=?, last_error_code='transient-not-accepted'
          WHERE provider_delivery_id=? AND state='claimed' AND revision=?
          RETURNING provider_delivery_id`,
          [row.updatedAt, row.providerDeliveryId, row.revision],
        );
        if (invalidated.length !== 1) {
          return yield* Effect.die(new Error("Prepared session recovery lost its delivery claim."));
        }
      }
      yield* sql.unsafe(`DELETE FROM main.${sessions} WHERE provider_delivery_id=?`, [
        row.providerDeliveryId,
      ]);
    }
    // Restore the original SQL byte-for-byte: existing DDL authority checks still apply.
    yield* sql.unsafe(source).unprepared;
  }
  // Old Planning writers inserted attestation before their delivery CAS. An archived
  // preparation must acquire a new immutable session binding before that insert.
  yield* sql`
    CREATE TRIGGER agent_control_prepared_session_archive_planning_attestation_validate
    BEFORE INSERT ON agent_control_initial_planning_delivery_attestations
    WHEN EXISTS (
      SELECT 1 FROM agent_control_prepared_session_archive archived
      WHERE archived.stage='initial-planning'
        AND archived.provider_delivery_id=NEW.provider_delivery_id
    ) AND NOT EXISTS (
      SELECT 1 FROM agent_control_initial_planning_session_evidence session
      WHERE session.provider_delivery_id=NEW.provider_delivery_id
        AND session.provider_instance_id=NEW.provider_instance_id
        AND session.model_selection_json=NEW.model_selection_json
        AND session.model_selection_fingerprint=NEW.model_selection_fingerprint
    )
    BEGIN SELECT RAISE(ABORT, 'recovered planning delivery requires session evidence'); END
  `;
});
