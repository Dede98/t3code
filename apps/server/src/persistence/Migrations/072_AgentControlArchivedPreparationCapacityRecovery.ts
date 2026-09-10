import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { sha256Utf8 } from "../../agentControl/initialPlanning/eventEvidence.ts";
import {
  archivedPreInvokeDeliveryPredicate,
  preInvokeDeliveryPredicate,
} from "../../agentControl/providerAdmission/preInvokeRecovery.ts";
import { PROVIDER_PRE_INVOKE_DDL_FINGERPRINTS } from "./068_AgentControlProviderPreInvokeRecovery.ts";

export const ARCHIVED_PREPARATION_ADMISSION_TRIGGER =
  "agent_control_provider_admission_current_validate_update";
export const ARCHIVED_PREPARATION_ADMISSION_FINGERPRINT =
  "5af2771fa1550c8e71ea0cd9ed7de8b235a1688ecf74d50a111d37d5f9bfa193";
export const PREPARED_SESSION_ARCHIVE_INSERT_GUARD = `CREATE TRIGGER agent_control_prepared_session_archive_no_insert
    BEFORE INSERT ON agent_control_prepared_session_archive
    BEGIN SELECT RAISE(ABORT, 'prepared session archive is sealed'); END`;

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ source: string }>`SELECT sql AS source FROM main.sqlite_schema
    WHERE type='trigger' AND name=${ARCHIVED_PREPARATION_ADMISSION_TRIGGER}`;
  const source = rows[0]?.source;
  if (
    source === undefined ||
    sha256Utf8(source) !==
      PROVIDER_PRE_INVOKE_DDL_FINGERPRINTS[ARCHIVED_PREPARATION_ADMISSION_TRIGGER]
  ) {
    return yield* Effect.die(
      new Error("Archived preparation recovery found divergent admission guard."),
    );
  }
  const previous = preInvokeDeliveryPredicate("intent", "NEW.updated_at");
  if (!source.includes(previous))
    return yield* Effect.die(new Error("Missing pre-invoke admission guard."));
  const updated = source.replace(
    previous,
    `(${previous}) OR (${archivedPreInvokeDeliveryPredicate("intent")})`,
  );
  if (sha256Utf8(updated) !== ARCHIVED_PREPARATION_ADMISSION_FINGERPRINT) {
    return yield* Effect.die(
      new Error(`Archived preparation recovery guard fingerprint: ${sha256Utf8(updated)}`),
    );
  }
  yield* sql.unsafe(`DROP TRIGGER main.${ARCHIVED_PREPARATION_ADMISSION_TRIGGER}`).unprepared;
  yield* sql.unsafe(updated).unprepared;
  // All historical rows were created by 070. Later writers cannot mint retirement proof.
  yield* sql.unsafe(PREPARED_SESSION_ARCHIVE_INSERT_GUARD).unprepared;
});
