import { IsoDateTime } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const ClaimedPreInvokeRecovery = Schema.Struct({
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
export const ProviderAdmissionPreInvokeRecovery = Schema.Union([
  ClaimedPreInvokeRecovery,
  Schema.Struct({
    previousOwnerId: Schema.NonEmptyString,
    previousFenceToken: Schema.Int.check(Schema.isGreaterThan(0)),
    previousLeaseExpiresAt: IsoDateTime,
    deliveryRevision: Schema.Int.check(Schema.isGreaterThan(0)),
    deliveryState: Schema.Literal("retry-wait"),
    deliveryAttestationAbsent: Schema.Literal(true),
    preparedSessionArchiveFingerprint: Schema.NonEmptyString,
  }),
]);
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

/** Migration 070 retired this precise pre-attempt delivery, not an arbitrary retry. */
export const preparedSessionArchiveBindingPredicate = (intent: string, archive: string) => `
  ${archive}.stage=${intent}.stage
  AND ${archive}.provider_delivery_id=${intent}.provider_delivery_id
  AND ${archive}.reason='session-prepared-before-delivery-cas'
  AND json_extract(${archive}.evidence_json,'$.provider_delivery_id')=${intent}.provider_delivery_id
  AND json_extract(${archive}.evidence_json,'$.thread_id')=${intent}.thread_id
  AND json_extract(${archive}.evidence_json,'$.provider_instance_id')=${intent}.provider_instance_id
  AND json_extract(${archive}.evidence_json,'$.model_selection_json')=CAST(${intent}.model_selection_json AS TEXT)
  AND json_extract(${archive}.evidence_json,'$.model_selection_fingerprint')=${intent}.model_selection_fingerprint
  AND json_type(${archive}.delivery_json,'$.revision')='integer'
  AND json_extract(${archive}.delivery_json,'$.revision')>0
  AND json_type(${archive}.delivery_json,'$.claim_generation')='integer'
  AND json_extract(${archive}.delivery_json,'$.claim_generation')>0
  AND (
    (json_extract(${archive}.delivery_json,'$.state')='claimed'
      AND length(json_extract(${archive}.delivery_json,'$.claim_owner_id'))>0
      AND length(json_extract(${archive}.delivery_json,'$.claim_expires_at'))>0)
    OR (json_extract(${archive}.delivery_json,'$.state')='retry-wait'
      AND json_type(${archive}.delivery_json,'$.claim_owner_id')='null'
      AND json_type(${archive}.delivery_json,'$.claim_expires_at')='null')
  )`;

export const archivedPreInvokeDeliveryPredicate = (intent: string) =>
  providerAdmissionDeliveryTables
    .map(
      ([stage, table, attestations]) => `(
    ${intent}.stage='${stage}' AND EXISTS (
      SELECT 1 FROM main.${table} delivery
      JOIN main.agent_control_prepared_session_archive archive
        ON archive.provider_delivery_id=delivery.provider_delivery_id
      WHERE ${preparedSessionArchiveBindingPredicate(intent, "archive")}
        AND delivery.handoff_id=${intent}.handoff_id AND delivery.thread_id=${intent}.thread_id
        AND delivery.provider_instance_id=${intent}.provider_instance_id
        AND delivery.state='retry-wait' AND delivery.claim_owner_id IS NULL AND delivery.claim_expires_at IS NULL
        AND delivery.revision=json_extract(archive.delivery_json,'$.revision')
          + CASE json_extract(archive.delivery_json,'$.state') WHEN 'claimed' THEN 1 ELSE 0 END
        AND delivery.claim_generation=json_extract(archive.delivery_json,'$.claim_generation')
        AND (json_extract(archive.delivery_json,'$.state')='retry-wait'
          OR (delivery.next_attempt_at=json_extract(archive.delivery_json,'$.updated_at')
            AND delivery.last_error_code='transient-not-accepted'))
        AND delivery.provider_turn_id IS NULL AND delivery.provider_accepted_at IS NULL
        AND delivery.provider_session_created_at IS NULL AND delivery.provider_resume_cursor_json IS NULL
        AND delivery.terminal_at IS NULL
        AND NOT EXISTS (SELECT 1 FROM main.${attestations} attestation
          WHERE attestation.provider_delivery_id=delivery.provider_delivery_id)
        AND NOT EXISTS (SELECT 1 FROM main.${table.replace(/_deliveries$/, "_session_evidence")} session
          WHERE session.provider_delivery_id=delivery.provider_delivery_id)
    )
  )`,
    )
    .join(" OR ");

export const PREPARED_SESSION_RECOVERY_DEADLINES_SQL = providerAdmissionDeliveryTables
  .map(
    ([stage, table]) => `
    SELECT current.admission_id AS "admissionId",current.stage,current.handoff_id AS "handoffId",
      current.provider_instance_id AS "providerInstanceId",'lease' AS "deadlineKind",
      current.lease_expires_at AS "deadlineAt"
    FROM main.agent_control_provider_admission_current current
    JOIN main.agent_control_provider_admission_intents intent ON intent.admission_id=current.admission_id
    JOIN main.${table} delivery ON delivery.provider_delivery_id=intent.provider_delivery_id
    WHERE current.stage='${stage}' AND current.status IN ('entered','quarantined')
      AND (${archivedPreInvokeDeliveryPredicate("intent")})
  `,
  )
  .join(" UNION ALL ");
