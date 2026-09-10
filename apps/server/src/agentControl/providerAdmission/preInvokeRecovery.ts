import { IsoDateTime } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const ProviderAdmissionPreInvokeRecovery = Schema.Struct({
  previousOwnerId: Schema.NonEmptyString,
  previousFenceToken: Schema.Int.check(Schema.isGreaterThan(0)),
  previousLeaseExpiresAt: IsoDateTime,
  deliveryRevision: Schema.Int.check(Schema.isGreaterThan(0)),
  claimOwnerId: Schema.NonEmptyString,
  claimGeneration: Schema.Int.check(Schema.isGreaterThan(0)),
  claimExpiresAt: IsoDateTime,
  deliveryState: Schema.Literal("claimed"),
  deliveryAttestationAbsent: Schema.Literal(true),
});
export type ProviderAdmissionPreInvokeRecovery = typeof ProviderAdmissionPreInvokeRecovery.Type;
export const decodePreInvokeRecovery = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      preInvokeRecovery: Schema.optional(ProviderAdmissionPreInvokeRecovery),
    }),
  ),
);

export const providerAdmissionDeliveryTables = [
  [
    "initial-planning",
    "agent_control_initial_planning_deliveries",
    "agent_control_initial_planning_delivery_attestations",
  ],
  [
    "implementation",
    "agent_control_implementation_deliveries",
    "agent_control_implementation_delivery_attestations",
  ],
  [
    "verification",
    "agent_control_verification_deliveries",
    "agent_control_verification_delivery_attestations",
  ],
] as const;

/** A turn cannot be invoked before its immutable delivery attestation is committed. */
export const preInvokeDeliveryPredicate = (intent: string, now: string) =>
  providerAdmissionDeliveryTables
    .map(
      ([stage, table, attestations]) => `(
    ${intent}.stage='${stage}' AND EXISTS (
      SELECT 1 FROM main.${table} delivery
      WHERE delivery.provider_delivery_id=${intent}.provider_delivery_id
        AND delivery.handoff_id=${intent}.handoff_id
        AND delivery.thread_id=${intent}.thread_id
        AND delivery.provider_instance_id=${intent}.provider_instance_id
        AND delivery.state='claimed' AND delivery.claim_expires_at<=${now}
        AND delivery.provider_turn_id IS NULL AND delivery.provider_accepted_at IS NULL
        AND delivery.provider_session_created_at IS NULL
        AND delivery.provider_resume_cursor_json IS NULL AND delivery.terminal_at IS NULL
        AND NOT EXISTS (SELECT 1 FROM main.${attestations} attestation
          WHERE attestation.provider_delivery_id=delivery.provider_delivery_id)
    )
  )`,
    )
    .join(" OR ");

export const PROVIDER_PRE_INVOKE_DEADLINES_SQL = providerAdmissionDeliveryTables
  .map(
    ([stage, table]) => `
  SELECT current.admission_id AS "admissionId",current.stage,current.handoff_id AS "handoffId",
    current.provider_instance_id AS "providerInstanceId",'lease' AS "deadlineKind",
    MAX(current.lease_expires_at,delivery.claim_expires_at) AS "deadlineAt"
  FROM main.agent_control_provider_admission_current current
  JOIN main.agent_control_provider_admission_intents intent ON intent.admission_id=current.admission_id
  JOIN main.${table} delivery ON delivery.provider_delivery_id=intent.provider_delivery_id
  WHERE current.stage='${stage}' AND current.status IN ('entered','quarantined')
    AND (${preInvokeDeliveryPredicate("intent", "delivery.claim_expires_at")})
`,
  )
  .join(" UNION ALL ");
