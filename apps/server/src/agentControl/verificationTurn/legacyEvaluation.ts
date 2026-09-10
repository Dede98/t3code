import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

/** Only evaluations captured at upgrade may replay the previous authority format. */
export const isLegacyVerificationEvaluation = Effect.fn("isLegacyVerificationEvaluation")(
  function* (sql: SqlClient.SqlClient, providerDeliveryId: string) {
    const rows = yield* sql`
      SELECT legacy.evaluation_id
      FROM main.agent_control_verification_legacy_evaluations legacy
      JOIN main.agent_control_verification_evaluation_evidence evaluation
        ON evaluation.evaluation_id = legacy.evaluation_id
        AND evaluation.provider_delivery_id = legacy.provider_delivery_id
        AND evaluation.authority_digest = legacy.authority_digest
      WHERE legacy.provider_delivery_id = ${providerDeliveryId}
    `;
    return rows.length === 1;
  },
);
