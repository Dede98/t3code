import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { sha256Utf8 } from "../../agentControl/initialPlanning/eventEvidence.ts";
import { preInvokeDeliveryPredicate } from "../../agentControl/providerAdmission/preInvokeRecovery.ts";
import { EXPECTED_PROVIDER_ADMISSION_DDL_FINGERPRINTS } from "./065_AgentControlProviderCapacityAdmission.ts";

export const PROVIDER_PRE_INVOKE_DDL_FINGERPRINTS: Readonly<Record<string, string>> = {
  agent_control_provider_admission_current_validate_update:
    "423952646534e018070a1294271fa8d934f806da4c7ae30a55fe8b68d7a5c05b",
  agent_control_provider_capacity_current_validate_update:
    "b817c60594b6b07ebc51539916c10a2730302aa58e1543ecba324227aba80d4b",
};

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const admission = "agent_control_provider_admission_current_validate_update";
  const capacity = "agent_control_provider_capacity_current_validate_update";
  for (const name of [admission, capacity]) {
    const rows = yield* sql<{ readonly source: string }>`
      SELECT sql AS source FROM main.sqlite_schema WHERE type='trigger' AND name=${name}
    `;
    const source = rows[0]?.source;
    if (
      source === undefined ||
      sha256Utf8(source) !== EXPECTED_PROVIDER_ADMISSION_DDL_FINGERPRINTS[name]
    ) {
      return yield* Effect.die(new Error("provider pre-invoke recovery found divergent trigger"));
    }
    const previous =
      name === admission
        ? "OR (OLD.status='admitted' AND NEW.status='admitted'"
        : "OR (OLD.active_state='admitted' AND NEW.active_state='admitted'";
    const added =
      name === admission
        ? `OR (OLD.status IN ('entered','quarantined') AND NEW.status='admitted'
              AND NEW.provider_fence_token=OLD.provider_fence_token+1
              AND OLD.lease_expires_at<=NEW.updated_at
              AND EXISTS (
                SELECT 1 FROM main.agent_control_provider_authority_evidence evidence
                JOIN main.agent_control_provider_admission_intents intent ON intent.admission_id=NEW.admission_id
                WHERE evidence.marker_id=NEW.admission_marker_id AND evidence.authority_kind='admission'
                  AND json_extract(evidence.payload_json,'$.preInvokeRecovery.previousOwnerId')=OLD.owner_id
                  AND json_extract(evidence.payload_json,'$.preInvokeRecovery.previousFenceToken')=OLD.provider_fence_token
                  AND json_extract(evidence.payload_json,'$.preInvokeRecovery.previousLeaseExpiresAt')=OLD.lease_expires_at
                  AND (${preInvokeDeliveryPredicate("intent", "NEW.updated_at")})
              ))
            ${previous}`
        : `OR (OLD.active_state IN ('entered','quarantined') AND NEW.active_state='admitted'
              AND NEW.last_fence_token=OLD.last_fence_token+1
              AND NEW.active_admission_id=OLD.active_admission_id
              AND OLD.active_lease_expires_at<=NEW.updated_at)
            ${previous}`;
    if (!source.includes(previous))
      return yield* Effect.die(new Error("provider recovery transition missing"));
    yield* sql.unsafe(`DROP TRIGGER main.${name}`).unprepared;
    const updated = source.replace(previous, added);
    if (sha256Utf8(updated) !== PROVIDER_PRE_INVOKE_DDL_FINGERPRINTS[name]) {
      return yield* Effect.die(new Error("provider recovery trigger fingerprint mismatch"));
    }
    yield* sql.unsafe(updated).unprepared;
  }
});
