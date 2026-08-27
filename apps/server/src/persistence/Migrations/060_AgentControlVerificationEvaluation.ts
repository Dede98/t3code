import { OrchestrationEvent as OrchestrationEventSchema } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { parseJsonStrict } from "../../agentControl/initialPlanning/eventEvidence.ts";
import { AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT } from "../../agentControl/verificationTurn/prompt.ts";
import {
  VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_CONFLICT,
  VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX,
  VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER,
} from "../../agentControl/verificationTurn/runtimeEventAuthority.ts";
import { AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT } from "../../agentControl/verificationTurn/verificationResult.ts";
import {
  VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS,
  verificationResultCompletionDetailDigest,
  verificationResultDeltaTextDigest,
  verificationResultOutputEvidenceDigest,
} from "../../agentControl/verificationTurn/runtimeEvidence.ts";
import { normalizeLegacyProviderRuntimeMessageCorrelationMetadata } from "../../orchestration/providerRuntimeMessageCorrelation.ts";

const canonicalUtf8 = (column: string) => `
  instr(${column}, char(0)) = 0
  AND NOT EXISTS (
    WITH RECURSIVE utf8(value) AS (
      SELECT ${column}
      UNION ALL
      SELECT substr(value, 2) FROM utf8 WHERE length(value) > 0
    )
    SELECT 1 FROM utf8
    WHERE length(value) > 0
      AND hex(CAST(substr(value, 1, 1) AS BLOB)) !=
        hex(CAST(char(unicode(value)) AS BLOB))
  )
`;
const text = (column: string) =>
  `typeof(${column}) = 'text' AND length(${column}) > 0 AND ${canonicalUtf8(column)}`;
const javascriptWhitespace = [
  9, 10, 11, 12, 13, 32, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201,
  8202, 8232, 8233, 8239, 8287, 12288, 65279,
]
  .map((codePoint) => `char(${codePoint})`)
  .join(" || ");
const canonicalCorrelationId = (column: string) =>
  `${orchestrationText(column)} AND trim(${column}, ${javascriptWhitespace}) = ${column}`;
const canonicalProviderInstanceId = (column: string) => `
  ${canonicalCorrelationId(column)}
  AND length(${column}) <= 64
  AND substr(${column}, 1, 1) GLOB '[A-Za-z]'
  AND ${column} NOT GLOB '*[^A-Za-z0-9_-]*'
`;
const nullableText = (column: string) => `(${column} IS NULL OR (${text(column)}))`;
const integer = (column: string) => `typeof(${column}) = 'integer'`;
const safeInteger = (column: string) =>
  `${integer(column)} AND ${column} BETWEEN 0 AND 9007199254740991`;
const nullableInteger = (column: string) => `(${column} IS NULL OR typeof(${column}) = 'integer')`;
const sha256 = (column: string) =>
  `${text(column)} AND length(${column}) = 64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;
const nullableSha256 = (column: string) => `(${column} IS NULL OR (${sha256(column)}))`;
const timestamp = (column: string) => `
  ${text(column)}
  AND length(${column}) = 24
  AND ${column} GLOB
    '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
  AND substr(${column}, 12, 2) BETWEEN '00' AND '23'
  AND strftime('%Y-%m-%dT%H:%M:%fZ', ${column}) = ${column}
`;
const canonicalJson = (column: string) =>
  `${text(column)} AND json_valid(${column}) = 1 AND json(${column}) = ${column}`;
const orchestrationText = (column: string) =>
  `typeof(${column}) = 'text'
    AND length(${column}) > 0
    AND instr(${column}, char(0)) = 0
    AND t3_fatal_utf8(CAST(${column} AS BLOB)) = 1`;
const nullableOrchestrationText = (column: string) =>
  `(${column} IS NULL OR (${orchestrationText(column)}))`;
const orchestrationJson = (column: string) => `
  CASE
    WHEN typeof(${column}) != 'text'
      OR length(${column}) = 0
      OR instr(${column}, char(0)) != 0
      THEN 0
    WHEN t3_fatal_utf8(CAST(${column} AS BLOB)) != 1 THEN 0
    WHEN json_valid(${column}) != 1 THEN 0
    ELSE json(${column}) = ${column}
  END
`;

const orchestrationEventStorage = (row = "NEW", minimumStreamVersion = 1) => `
  ${orchestrationText(`${row}.event_id`)}
  AND ${orchestrationText(`${row}.aggregate_kind`)}
  AND ${row}.aggregate_kind IN ('project', 'thread')
  AND ${orchestrationText(`${row}.stream_id`)}
  AND ${integer(`${row}.stream_version`)}
  AND ${row}.stream_version >= ${minimumStreamVersion}
  AND ${orchestrationText(`${row}.event_type`)}
  AND ${row}.event_type IN (
    'project.created', 'project.meta-updated', 'project.deleted',
    'thread.created', 'thread.deleted', 'thread.archived', 'thread.unarchived',
    'thread.meta-updated', 'thread.runtime-mode-set', 'thread.interaction-mode-set',
    'thread.message-sent', 'thread.verification-result-fragment-captured',
    'thread.turn-start-requested', 'thread.turn-interrupt-requested',
    'thread.approval-response-requested', 'thread.user-input-response-requested',
    'thread.checkpoint-revert-requested', 'thread.reverted',
    'thread.session-stop-requested', 'thread.session-set',
    'thread.proposed-plan-upserted', 'thread.turn-diff-completed',
    'thread.activity-appended', 'thread.agent-control-bound',
    'thread.agent-control-state-set'
  )
  AND ${timestamp(`${row}.occurred_at`)}
  AND ${nullableOrchestrationText(`${row}.command_id`)}
  AND ${nullableOrchestrationText(`${row}.causation_event_id`)}
  AND ${nullableOrchestrationText(`${row}.correlation_id`)}
  AND ${orchestrationText(`${row}.actor_kind`)}
  AND ${row}.actor_kind IN ('client', 'server', 'provider')
  AND ${orchestrationJson(`${row}.payload_json`)}
  AND json_type(${row}.payload_json) = 'object'
  AND ${orchestrationJson(`${row}.metadata_json`)}
  AND json_type(${row}.metadata_json) = 'object'
  AND ${integer(`${row}.sequence`)}
  AND ${row}.sequence >= 1
`;

interface HistoricalSourceRow {
  readonly sequence: number;
  readonly eventId: string;
  readonly aggregateKind: "project" | "thread";
  readonly streamId: string;
  readonly eventType: string;
  readonly occurredAt: string;
  readonly commandId: string | null;
  readonly causationEventId: string | null;
  readonly correlationId: string | null;
  readonly actorKind: "client" | "server" | "provider";
  readonly payloadJson: string;
  readonly metadataJson: string;
}

const isOrchestrationEvent = Schema.is(OrchestrationEventSchema);

const historicalSourceRowIsValid = (row: HistoricalSourceRow): boolean => {
  let payload: unknown;
  let metadata: unknown;
  try {
    payload = parseJsonStrict(row.payloadJson);
    metadata = parseJsonStrict(row.metadataJson);
  } catch {
    return false;
  }

  const event = {
    sequence: row.sequence,
    eventId: row.eventId,
    aggregateKind: row.aggregateKind,
    aggregateId: row.streamId,
    type: row.eventType,
    occurredAt: row.occurredAt,
    commandId: row.commandId,
    causationEventId: row.causationEventId,
    correlationId: row.correlationId,
    payload,
    metadata: normalizeLegacyProviderRuntimeMessageCorrelationMetadata(metadata),
  };
  if (!isOrchestrationEvent(event)) return false;

  if (event.type === "thread.message-sent") {
    if (event.payload.threadId !== row.streamId) return false;
    const runtime = event.metadata.providerRuntimeMessage;
    const capture = event.metadata.verificationResultCapture;
    if (runtime !== undefined) {
      if (
        row.actorKind !== "provider" ||
        row.commandId === null ||
        !row.commandId.startsWith(`provider:${runtime.runtimeEventId}:`) ||
        row.causationEventId !== null ||
        row.correlationId !== row.commandId ||
        event.payload.turnId !== runtime.providerTurnId
      ) {
        return false;
      }
    }
    if (
      capture !== undefined &&
      (runtime === undefined || capture.disposition !== "presentation")
    ) {
      return false;
    }
    return capture === undefined;
  }

  if (event.type === "thread.verification-result-fragment-captured") {
    // Runtime capture authority and its schema were introduced by migration 060.
    // A schema-059 database containing one is partially migrated and must fail closed.
    return false;
  }

  if (event.type === "thread.session-set") {
    const seal = event.metadata.verificationResultSource;
    const lifecycle = event.metadata.providerRuntimeLifecycle;
    if (seal !== undefined) {
      // Verification result seals are also migration-060-only authority.
      return false;
    }
    if (lifecycle === undefined) return true;
    if (
      row.actorKind !== "provider" ||
      row.commandId === null ||
      !row.commandId.startsWith(`provider:${lifecycle.runtimeEventId}:thread-session-set:`) ||
      row.causationEventId !== null ||
      row.correlationId !== row.commandId ||
      event.payload.threadId !== row.streamId ||
      event.payload.session.threadId !== row.streamId ||
      event.payload.session.providerInstanceId !== lifecycle.providerInstanceId
    ) {
      return false;
    }
    return true;
  }

  return true;
};

const verificationSealCommandId = (row = "NEW") => {
  const prefix = `'provider:' || json_extract(
    ${row}.metadata_json, '$.providerRuntimeLifecycle.runtimeEventId'
  ) || ':thread-session-set:'`;
  const uuid = `substr(${row}.command_id, length(${prefix}) + 1)`;
  const compactUuid = `replace(${uuid}, '-', '')`;
  return `
    ${row}.command_id LIKE ${prefix} || '%'
    AND length(${row}.command_id) = length(${prefix}) + 36
    AND ${uuid} = lower(${uuid})
    AND substr(${uuid}, 9, 1) = '-'
    AND substr(${uuid}, 14, 1) = '-'
    AND substr(${uuid}, 15, 1) = '4'
    AND substr(${uuid}, 19, 1) = '-'
    AND substr(${uuid}, 20, 1) IN ('8', '9', 'a', 'b')
    AND substr(${uuid}, 24, 1) = '-'
    AND length(${compactUuid}) = 32
    AND ${compactUuid} NOT GLOB '*[^0-9a-f]*'
  `;
};

const resultContractPredicate = (row = "NEW") => `
  (
    ${row}.prompt_template_version IS NULL
    AND ${row}.prompt_contract_fingerprint IS NULL
    AND ${row}.result_schema_version IS NULL
    AND ${row}.result_schema_fingerprint IS NULL
  ) OR (
    ${row}.template_version = 'agent-control-verification-prompt-v1'
    AND ${row}.prompt_template_version = 'agent-control-verification-prompt-v2'
    AND ${sha256(`${row}.prompt_contract_fingerprint`)}
    AND ${row}.prompt_contract_fingerprint =
      '${AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT}'
    AND ${row}.result_schema_version = 'agent-control-verification-result-v1'
    AND ${sha256(`${row}.result_schema_fingerprint`)}
    AND ${row}.result_schema_fingerprint =
      '${AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT}'
  )
`;

const previousVerificationCaptureByteLength = `COALESCE((
  SELECT CASE json_extract(prior.payload_json, '$.fragment.kind')
    WHEN 'delta' THEN json_extract(prior.payload_json, '$.fragment.cumulativeSourceByteLength')
    WHEN 'completion' THEN json_extract(prior.payload_json, '$.fragment.outputByteLength')
  END
  FROM main.orchestration_events prior
  WHERE prior.stream_id IS NEW.stream_id
    AND prior.stream_version < NEW.stream_version
    AND prior.event_type = 'thread.verification-result-fragment-captured'
    AND json_extract(prior.payload_json, '$.messageId') IS json_extract(
      NEW.payload_json, '$.messageId'
    )
    AND json_extract(
      prior.metadata_json, '$.verificationResultCapture.providerDeliveryId'
    ) IS json_extract(
      NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
    )
  ORDER BY prior.stream_version DESC
  LIMIT 1
), 0)`;

const previousVerificationCaptureFragmentOrdinal = `COALESCE((
  SELECT json_extract(prior.payload_json, '$.fragment.fragmentOrdinal')
  FROM main.orchestration_events prior
  WHERE prior.stream_id IS NEW.stream_id
    AND prior.stream_version < NEW.stream_version
    AND prior.event_type = 'thread.verification-result-fragment-captured'
    AND json_extract(prior.payload_json, '$.messageId') IS json_extract(
      NEW.payload_json, '$.messageId'
    )
    AND json_extract(
      prior.metadata_json, '$.verificationResultCapture.providerDeliveryId'
    ) IS json_extract(
      NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
    )
  ORDER BY prior.stream_version DESC
  LIMIT 1
), 0)`;

const previousVerificationCaptureEvidenceDigest = `COALESCE((
  SELECT json_extract(prior.payload_json, '$.fragment.cumulativeEvidenceDigest')
  FROM main.orchestration_events prior
  WHERE prior.stream_id IS NEW.stream_id
    AND prior.stream_version < NEW.stream_version
    AND prior.event_type = 'thread.verification-result-fragment-captured'
    AND json_extract(prior.payload_json, '$.messageId') IS json_extract(
      NEW.payload_json, '$.messageId'
    )
    AND json_extract(
      prior.metadata_json, '$.verificationResultCapture.providerDeliveryId'
    ) IS json_extract(
      NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
    )
  ORDER BY prior.stream_version DESC
  LIMIT 1
), '${VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS}')`;

const previousVerificationCaptureStoredByteLength = `COALESCE((
  SELECT sum(json_extract(prior.payload_json, '$.fragment.prefixByteLength'))
  FROM main.orchestration_events prior
  WHERE prior.stream_id IS NEW.stream_id
    AND prior.stream_version < NEW.stream_version
    AND prior.event_type = 'thread.verification-result-fragment-captured'
    AND json_extract(prior.payload_json, '$.messageId') IS json_extract(
      NEW.payload_json, '$.messageId'
    )
    AND json_extract(prior.payload_json, '$.fragment.kind') = 'delta'
    AND json_extract(
      prior.metadata_json, '$.verificationResultCapture.providerDeliveryId'
    ) IS json_extract(
      NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
    )
), 0)`;

const evidenceStorage = (row = "NEW") =>
  [
    ...[
      "evaluation_id",
      "evidence_id",
      "disposition",
      "source_disposition",
      "project_id",
      "task_id",
      "source_identity_fingerprint",
      "worktree_reservation_id",
      "worktree_event_id",
      "worktree_ownership_fingerprint",
      "worktree_verified_at",
      "worktree_path",
      "branch",
      "stage_run_id",
      "attempt_id",
      "lease_id",
      "lease_holder_id",
      "controlled_thread_reservation_id",
      "handoff_id",
      "provider_delivery_id",
      "thread_id",
      "provider_instance_id",
      "provider_turn_id",
      "prompt_template_version",
      "result_schema_version",
      "terminal_event_id",
      "terminal_state",
      "start_marker_id",
      "receipt_id",
      "marker_id",
    ].map((column) => text(`${row}.${column}`)),
    ...[
      "evaluation_fingerprint",
      "authority_digest",
      "handoff_fingerprint",
      "model_selection_fingerprint",
      "prompt_contract_fingerprint",
      "prompt_digest",
      "result_schema_fingerprint",
      "terminal_observation_digest",
    ].map((column) => sha256(`${row}.${column}`)),
    ...["verdict", "error_code", "source_message_id", "source_event_id"].map((column) =>
      nullableText(`${row}.${column}`),
    ),
    ...["raw_output_digest", "semantic_result_digest"].map((column) =>
      nullableSha256(`${row}.${column}`),
    ),
    ...[
      "revision",
      "task_revision",
      "github_intake_sequence",
      "worktree_revision",
      "worktree_event_sequence",
      "worktree_event_stream_version",
      "fence_token",
      "terminal_sequence",
      "terminal_stream_version",
      "output_byte_length",
    ].map((column) => integer(`${row}.${column}`)),
    ...["source_event_sequence", "source_event_stream_version"].map((column) =>
      nullableInteger(`${row}.${column}`),
    ),
    canonicalJson(`${row}.authority_json`),
    timestamp(`${row}.evaluated_at`),
  ].join(" AND ");

const receiptStorage = (row = "NEW") =>
  [
    ...[
      "receipt_id",
      "evaluation_id",
      "evidence_id",
      "marker_id",
      "provider_delivery_id",
      "provider_instance_id",
      "provider_turn_id",
      "terminal_event_id",
      "disposition",
      "source_disposition",
      "status",
    ].map((column) => text(`${row}.${column}`)),
    ...["evaluation_fingerprint", "terminal_observation_digest"].map((column) =>
      sha256(`${row}.${column}`),
    ),
    ...["source_message_id", "source_event_id", "verdict", "error_code"].map((column) =>
      nullableText(`${row}.${column}`),
    ),
    nullableSha256(`${row}.raw_output_digest`),
    integer(`${row}.output_byte_length`),
    timestamp(`${row}.accepted_at`),
  ].join(" AND ");

const markerStorage = (row = "NEW") =>
  [
    ...["marker_id", "evaluation_id", "evidence_id", "receipt_id", "provider_delivery_id"].map(
      (column) => text(`${row}.${column}`),
    ),
    sha256(`${row}.evaluation_fingerprint`),
    integer(`${row}.marker_version`),
    timestamp(`${row}.committed_at`),
  ].join(" AND ");

export type Migration060FaultPoint =
  | "before-copy"
  | "after-copy"
  | "after-runtime-authority-install"
  | "after-install";

export interface Migration060TestHooks {
  readonly sourcePreflightPageSize?: number;
  readonly onSourcePreflightPage?: (page: {
    readonly afterSequence: number;
    readonly rowCount: number;
  }) => void;
}

const SOURCE_PREFLIGHT_PAGE_SIZE = 64;

const migration060TriggerAudit = [
  [
    "agent_control_verification_handoff_result_contract_storage_validate",
    "agent_control_verification_handoff_intents",
    "invalid verification result contract storage",
  ],
  [
    "agent_control_verification_handoff_result_contract_update_storage_validate",
    "agent_control_verification_handoff_intents",
    "invalid verification result contract storage",
  ],
  [
    "agent_control_verification_evaluation_evidence_storage_validate",
    "agent_control_verification_evaluation_evidence",
    "invalid verification evaluation evidence storage",
  ],
  [
    "agent_control_verification_evaluation_receipt_storage_validate",
    "agent_control_verification_evaluation_receipts",
    "invalid verification evaluation receipt storage",
  ],
  [
    "agent_control_verification_evaluation_marker_storage_validate",
    "agent_control_verification_evaluation_markers",
    "invalid verification evaluation marker storage",
  ],
  [
    "agent_control_orchestration_event_storage_validate",
    "orchestration_events",
    "invalid orchestration event storage",
  ],
  [
    "agent_control_orchestration_event_update_storage_validate",
    "orchestration_events",
    "invalid orchestration event storage",
  ],
  [
    "agent_control_orchestration_message_structure_validate",
    "orchestration_events",
    "invalid orchestration message structure",
  ],
  [
    "agent_control_verification_result_source_seal_validate",
    "orchestration_events",
    "invalid verification result source seal",
  ],
  [
    "agent_control_verification_result_fragment_structure_validate",
    "orchestration_events",
    "invalid verification result fragment structure",
  ],
  [
    "agent_control_verification_result_capture_validate",
    "orchestration_events",
    "invalid verification result capture authority",
  ],
  [
    "agent_control_verification_result_post_seal_reject",
    "orchestration_events",
    "verification result source is sealed",
  ],
  [
    "agent_control_verification_result_authority_no_update",
    "orchestration_events",
    "verification result authority is immutable",
  ],
  [
    "agent_control_verification_result_authority_no_replace",
    "orchestration_events",
    "verification result authority is immutable",
  ],
  [
    "agent_control_verification_result_authority_no_delete",
    "orchestration_events",
    "verification result authority is immutable",
  ],
  [
    VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER,
    "orchestration_events",
    VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_CONFLICT,
  ],
  [
    "agent_control_verification_evaluation_evidence_validate",
    "agent_control_verification_evaluation_evidence",
    "verification evaluation authority is inconsistent",
  ],
  [
    "agent_control_verification_evaluation_receipt_validate",
    "agent_control_verification_evaluation_receipts",
    "verification evaluation receipt is inconsistent",
  ],
  [
    "agent_control_verification_evaluation_marker_validate",
    "agent_control_verification_evaluation_markers",
    "verification evaluation marker is inconsistent",
  ],
  [
    "agent_control_verification_evaluation_evidence_no_update",
    "agent_control_verification_evaluation_evidence",
    "verification evaluation evidence is immutable",
  ],
  [
    "agent_control_verification_evaluation_evidence_no_delete",
    "agent_control_verification_evaluation_evidence",
    "verification evaluation evidence is immutable",
  ],
  [
    "agent_control_verification_evaluation_receipts_no_update",
    "agent_control_verification_evaluation_receipts",
    "verification evaluation evidence is immutable",
  ],
  [
    "agent_control_verification_evaluation_receipts_no_delete",
    "agent_control_verification_evaluation_receipts",
    "verification evaluation evidence is immutable",
  ],
  [
    "agent_control_verification_evaluation_markers_no_update",
    "agent_control_verification_evaluation_markers",
    "verification evaluation evidence is immutable",
  ],
  [
    "agent_control_verification_evaluation_markers_no_delete",
    "agent_control_verification_evaluation_markers",
    "verification evaluation evidence is immutable",
  ],
] as const;

const normalizeSchemaSql = (sql: string): string => sql.replace(/\s+/gu, " ").trim();

export const makeMigration060 = (
  faultPoint?: Migration060FaultPoint,
  _testHooks?: Migration060TestHooks,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const injectFault = (point: Migration060FaultPoint) =>
      faultPoint === point
        ? Effect.die(new Error(`migration 060 injected ${point} failure`))
        : Effect.void;

    const udfPreflight = yield* sql<{
      readonly valid: number;
      readonly replacement: number;
      readonly invalid: number;
      readonly blobOnly: number;
      readonly deltaDigest: string;
      readonly completionDigest: string;
      readonly evidenceDigest: string;
    }>`
      SELECT t3_fatal_utf8(CAST('valid utf8' AS BLOB)) AS valid,
        t3_fatal_utf8(CAST(${`�`} AS BLOB)) AS replacement,
        t3_fatal_utf8(CAST(X'80' AS BLOB)) AS invalid,
        t3_fatal_utf8('valid utf8') AS "blobOnly",
        t3_verification_delta_digest('delta') AS "deltaDigest",
        t3_verification_completion_digest('completion') AS "completionDigest",
        t3_verification_evidence_digest(
          ${VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS}, 'delta', 1, 1,
          ${Buffer.byteLength("delta", "utf8")}, ${verificationResultDeltaTextDigest("delta")}
        ) AS "evidenceDigest"
    `;
    if (
      udfPreflight.length !== 1 ||
      udfPreflight[0]?.valid !== 1 ||
      udfPreflight[0]?.replacement !== 1 ||
      udfPreflight[0]?.invalid !== 0 ||
      udfPreflight[0]?.blobOnly !== 0 ||
      udfPreflight[0]?.deltaDigest !== verificationResultDeltaTextDigest("delta") ||
      udfPreflight[0]?.completionDigest !==
        verificationResultCompletionDetailDigest("completion") ||
      udfPreflight[0]?.evidenceDigest !==
        verificationResultOutputEvidenceDigest({
          previousDigest: VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS,
          fragmentKind: "delta",
          fragmentOrdinal: 1,
          fullByteLength: Buffer.byteLength("delta", "utf8"),
          fullDigest: verificationResultDeltaTextDigest("delta"),
          detailPresent: true,
        })
    ) {
      return yield* Effect.die(new Error("migration 060 SQLite function preflight failed"));
    }

    const invalidHistory = yield* sql.unsafe<{ readonly sequence: number }>(`
      SELECT history.sequence
      FROM main.orchestration_events history
      WHERE NOT COALESCE((${orchestrationEventStorage("history", 0)}), 0)
      ORDER BY history.sequence
      LIMIT 1
    `);
    if (invalidHistory.length !== 0) {
      return yield* Effect.die(new Error("migration 060 rejected orchestration history"));
    }

    const invalidStreamProgression = yield* sql.unsafe<{ readonly sequence: number }>(`
      WITH stream_progression AS (
        SELECT sequence, stream_version,
          row_number() OVER (
            PARTITION BY aggregate_kind, stream_id ORDER BY sequence
          ) AS stream_ordinal,
          lag(stream_version) OVER (
            PARTITION BY aggregate_kind, stream_id ORDER BY sequence
          ) AS previous_stream_version
        FROM main.orchestration_events
      )
      SELECT sequence
      FROM stream_progression
      WHERE (stream_ordinal = 1 AND stream_version NOT IN (0, 1))
        OR (stream_ordinal > 1 AND stream_version != previous_stream_version + 1)
      ORDER BY sequence
      LIMIT 1
    `);
    if (invalidStreamProgression.length !== 0) {
      return yield* Effect.die(
        new Error("migration 060 rejected orchestration stream progression"),
      );
    }

    const sourcePreflightPageSize =
      _testHooks?.sourcePreflightPageSize ?? SOURCE_PREFLIGHT_PAGE_SIZE;
    if (!Number.isSafeInteger(sourcePreflightPageSize) || sourcePreflightPageSize < 1) {
      return yield* Effect.die(new Error("migration 060 source page size is invalid"));
    }

    let afterSequence = 0;
    while (true) {
      const historicalSources = yield* sql<HistoricalSourceRow>`
        SELECT sequence, event_id AS "eventId", aggregate_kind AS "aggregateKind",
          stream_id AS "streamId", event_type AS "eventType", occurred_at AS "occurredAt",
          command_id AS "commandId", causation_event_id AS "causationEventId",
          correlation_id AS "correlationId", actor_kind AS "actorKind",
          payload_json AS "payloadJson", metadata_json AS "metadataJson"
        FROM main.orchestration_events
        WHERE sequence > ${afterSequence}
          AND CAST(event_type AS BLOB) IN (
            CAST('thread.message-sent' AS BLOB),
            CAST('thread.verification-result-fragment-captured' AS BLOB),
            CAST('thread.session-set' AS BLOB)
          )
        ORDER BY sequence
        LIMIT ${sourcePreflightPageSize}
      `;
      _testHooks?.onSourcePreflightPage?.({
        afterSequence,
        rowCount: historicalSources.length,
      });
      if (historicalSources.length === 0) break;
      if (historicalSources.some((row) => !historicalSourceRowIsValid(row))) {
        return yield* Effect.die(new Error("migration 060 rejected verification source history"));
      }

      const pageLastSequence = historicalSources.at(-1)!.sequence;
      const invalidRelationalSource = yield* sql<{ readonly sequence: number }>`
        SELECT source.sequence
        FROM main.orchestration_events source
        WHERE source.sequence > ${afterSequence}
          AND source.sequence <= ${pageLastSequence}
          AND (
            (
              source.event_type = 'thread.message-sent'
              AND EXISTS (
                SELECT 1 FROM main.agent_control_verification_handoff_intents candidate
                WHERE candidate.message_event_id IS source.event_id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM main.agent_control_verification_handoff_intents intent
                JOIN main.agent_control_verification_handoff_receipts receipt
                  ON receipt.handoff_id = intent.handoff_id
                 AND receipt.handoff_fingerprint = intent.handoff_fingerprint
                 AND receipt.materialization_evidence_id = intent.materialization_evidence_id
                 AND receipt.controlled_thread_reservation_id =
                   intent.controlled_thread_reservation_id
                 AND receipt.thread_id = intent.thread_id
                 AND receipt.turn_request_command_id = intent.turn_request_command_id
                 AND receipt.message_id = intent.message_id
                 AND receipt.provider_delivery_id = intent.provider_delivery_id
                 AND receipt.status = 'accepted'
                JOIN main.agent_control_verification_handoff_accepted accepted
                  ON accepted.handoff_id = receipt.handoff_id
                 AND accepted.handoff_fingerprint = receipt.handoff_fingerprint
                 AND accepted.materialization_evidence_id = receipt.materialization_evidence_id
                 AND accepted.controlled_thread_reservation_id =
                   receipt.controlled_thread_reservation_id
                 AND accepted.thread_id = receipt.thread_id
                 AND accepted.turn_request_command_id = receipt.turn_request_command_id
                 AND accepted.message_id = receipt.message_id
                 AND accepted.provider_delivery_id = receipt.provider_delivery_id
                JOIN main.agent_control_verification_deliveries delivery
                  ON delivery.handoff_id = accepted.handoff_id
                 AND delivery.handoff_fingerprint = accepted.handoff_fingerprint
                 AND delivery.materialization_evidence_id = accepted.materialization_evidence_id
                 AND delivery.controlled_thread_reservation_id =
                   accepted.controlled_thread_reservation_id
                 AND delivery.thread_id = accepted.thread_id
                 AND delivery.turn_request_command_id = accepted.turn_request_command_id
                 AND delivery.message_id = accepted.message_id
                 AND delivery.provider_delivery_id = accepted.provider_delivery_id
                 AND delivery.stage_run_id = intent.stage_run_id
                 AND delivery.attempt_id = intent.attempt_id
                 AND delivery.lease_id = intent.lease_id
                 AND delivery.lease_holder_id = intent.lease_holder_id
                 AND delivery.fence_token = intent.fence_token
                 AND delivery.provider_instance_id = intent.provider_instance_id
                 AND delivery.model_selection_fingerprint = intent.model_selection_fingerprint
                JOIN main.agent_control_verification_turn_accepted turn_accepted
                  ON turn_accepted.handoff_id = accepted.handoff_id
                 AND turn_accepted.handoff_fingerprint = accepted.handoff_fingerprint
                 AND turn_accepted.controlled_thread_reservation_id =
                   accepted.controlled_thread_reservation_id
                 AND turn_accepted.thread_id = accepted.thread_id
                 AND turn_accepted.turn_request_command_id = accepted.turn_request_command_id
                 AND turn_accepted.message_id = accepted.message_id
                 AND turn_accepted.message_event_id = intent.message_event_id
                 AND turn_accepted.message_event_sequence = source.sequence
                WHERE intent.message_event_id IS source.event_id
                  AND intent.template_version = 'agent-control-verification-prompt-v1'
                  AND intent.thread_id IS source.stream_id
                  AND intent.message_id IS json_extract(source.payload_json, '$.messageId')
                  AND intent.turn_request_command_id IS source.command_id
                  AND source.actor_kind = 'client'
                  AND source.causation_event_id IS NULL
                  AND source.correlation_id IS source.command_id
              )
            )
            OR (
              source.event_type IN ('thread.message-sent', 'thread.session-set')
              AND (
                json_type(source.metadata_json, '$.providerRuntimeMessage') = 'object'
                OR json_type(source.metadata_json, '$.providerRuntimeLifecycle') = 'object'
              )
              AND EXISTS (
                SELECT 1 FROM main.agent_control_verification_deliveries candidate
                WHERE candidate.thread_id IS source.stream_id
              )
              AND NOT EXISTS (
                SELECT 1
                FROM main.agent_control_verification_deliveries delivery
                JOIN main.agent_control_verification_handoff_intents intent
                  ON intent.handoff_id = delivery.handoff_id
                 AND intent.handoff_fingerprint = delivery.handoff_fingerprint
                 AND intent.materialization_evidence_id = delivery.materialization_evidence_id
                 AND intent.controlled_thread_reservation_id =
                   delivery.controlled_thread_reservation_id
                 AND intent.thread_id = delivery.thread_id
                 AND intent.stage_run_id = delivery.stage_run_id
                 AND intent.attempt_id = delivery.attempt_id
                 AND intent.lease_id = delivery.lease_id
                 AND intent.lease_holder_id = delivery.lease_holder_id
                 AND intent.fence_token = delivery.fence_token
                 AND intent.provider_instance_id = delivery.provider_instance_id
                 AND intent.model_selection_fingerprint = delivery.model_selection_fingerprint
                 AND intent.turn_request_command_id = delivery.turn_request_command_id
                 AND intent.message_id = delivery.message_id
                 AND intent.provider_delivery_id = delivery.provider_delivery_id
                 AND intent.template_version = 'agent-control-verification-prompt-v1'
                JOIN main.agent_control_verification_handoff_receipts receipt
                  ON receipt.handoff_id = intent.handoff_id
                 AND receipt.handoff_fingerprint = intent.handoff_fingerprint
                 AND receipt.materialization_evidence_id = intent.materialization_evidence_id
                 AND receipt.controlled_thread_reservation_id =
                   intent.controlled_thread_reservation_id
                 AND receipt.thread_id = intent.thread_id
                 AND receipt.turn_request_command_id = intent.turn_request_command_id
                 AND receipt.message_id = intent.message_id
                 AND receipt.provider_delivery_id = intent.provider_delivery_id
                 AND receipt.status = 'accepted'
                JOIN main.agent_control_verification_handoff_accepted accepted
                  ON accepted.handoff_id = receipt.handoff_id
                 AND accepted.handoff_fingerprint = receipt.handoff_fingerprint
                 AND accepted.materialization_evidence_id = receipt.materialization_evidence_id
                 AND accepted.controlled_thread_reservation_id =
                   receipt.controlled_thread_reservation_id
                 AND accepted.thread_id = receipt.thread_id
                 AND accepted.turn_request_command_id = receipt.turn_request_command_id
                 AND accepted.message_id = receipt.message_id
                 AND accepted.provider_delivery_id = receipt.provider_delivery_id
                JOIN main.agent_control_verification_turn_accepted turn_accepted
                  ON turn_accepted.handoff_id = accepted.handoff_id
                 AND turn_accepted.handoff_fingerprint = accepted.handoff_fingerprint
                 AND turn_accepted.controlled_thread_reservation_id =
                   accepted.controlled_thread_reservation_id
                 AND turn_accepted.thread_id = accepted.thread_id
                 AND turn_accepted.turn_request_command_id = accepted.turn_request_command_id
                 AND turn_accepted.message_id = accepted.message_id
                 AND turn_accepted.message_event_id = intent.message_event_id
                JOIN main.agent_control_verification_session_evidence session
                  ON session.provider_delivery_id = delivery.provider_delivery_id
                 AND session.thread_id = delivery.thread_id
                 AND session.provider_instance_id = delivery.provider_instance_id
                 AND session.runtime_mode = delivery.runtime_mode
                 AND session.model_selection_fingerprint = delivery.model_selection_fingerprint
                JOIN main.agent_control_verification_delivery_attestations attestation
                  ON attestation.provider_delivery_id = delivery.provider_delivery_id
                 AND attestation.provider_instance_id = delivery.provider_instance_id
                 AND attestation.model_selection_fingerprint =
                   delivery.model_selection_fingerprint
                JOIN main.agent_control_verification_stage_started_evidence started
                  ON started.provider_delivery_id = delivery.provider_delivery_id
                 AND started.handoff_id = delivery.handoff_id
                 AND started.handoff_fingerprint = delivery.handoff_fingerprint
                 AND started.controlled_thread_reservation_id =
                   delivery.controlled_thread_reservation_id
                 AND started.thread_id = delivery.thread_id
                 AND started.stage_run_id = delivery.stage_run_id
                 AND started.attempt_id = delivery.attempt_id
                 AND started.lease_id = delivery.lease_id
                 AND started.lease_holder_id = delivery.lease_holder_id
                 AND started.fence_token = delivery.fence_token
                 AND started.provider_instance_id = delivery.provider_instance_id
                 AND started.provider_turn_id = delivery.provider_turn_id
                 AND started.model_selection_fingerprint = delivery.model_selection_fingerprint
                JOIN main.agent_control_verification_stage_started_receipts started_receipt
                  ON started_receipt.start_evidence_id = started.start_evidence_id
                 AND started_receipt.start_command_id = started.start_command_id
                 AND started_receipt.start_fingerprint = started.start_fingerprint
                 AND started_receipt.provider_delivery_id = started.provider_delivery_id
                 AND started_receipt.stage_event_id = started.stage_event_id
                 AND started_receipt.stage_event_sequence = started.stage_event_sequence
                JOIN main.agent_control_verification_stage_started_markers started_marker
                  ON started_marker.start_evidence_id = started.start_evidence_id
                 AND started_marker.start_receipt_id = started_receipt.start_receipt_id
                 AND started_marker.start_command_id = started.start_command_id
                 AND started_marker.start_fingerprint = started.start_fingerprint
                 AND started_marker.provider_delivery_id = started.provider_delivery_id
                 AND started_marker.stage_event_id = started.stage_event_id
                 AND started_marker.stage_event_sequence = started.stage_event_sequence
                WHERE delivery.thread_id IS source.stream_id
                  AND delivery.provider_instance_id IS COALESCE(
                    json_extract(
                      source.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
                    ),
                    json_extract(
                      source.metadata_json, '$.providerRuntimeLifecycle.providerInstanceId'
                    )
                  )
                  AND delivery.provider_turn_id IS COALESCE(
                    json_extract(source.metadata_json, '$.providerRuntimeMessage.providerTurnId'),
                    json_extract(source.metadata_json, '$.providerRuntimeLifecycle.providerTurnId')
                  )
                  AND delivery.state IN ('provider-started', 'completed', 'failed', 'interrupted')
                  AND source.actor_kind = 'provider'
                  AND source.causation_event_id IS NULL
                  AND source.correlation_id IS source.command_id
                  AND (
                    (
                      source.event_type = 'thread.message-sent'
                      AND json_extract(source.payload_json, '$.threadId') IS delivery.thread_id
                      AND json_extract(source.payload_json, '$.turnId') IS delivery.provider_turn_id
                    )
                    OR (
                      source.event_type = 'thread.session-set'
                      AND json_extract(source.payload_json, '$.threadId') IS delivery.thread_id
                      AND json_extract(
                        source.payload_json, '$.session.providerInstanceId'
                      ) IS delivery.provider_instance_id
                      AND (
                        json_extract(
                          source.metadata_json, '$.providerRuntimeLifecycle.runtimeEventType'
                        ) = 'turn.started'
                        OR (
                          delivery.terminal_event_id IS json_extract(
                            source.metadata_json, '$.providerRuntimeLifecycle.runtimeEventId'
                          )
                          AND delivery.terminal_event_type IS json_extract(
                            source.metadata_json, '$.providerRuntimeLifecycle.runtimeEventType'
                          )
                          AND (
                            (
                              delivery.terminal_event_type = 'turn.completed'
                              AND delivery.terminal_provider_state IS json_extract(
                                source.metadata_json,
                                '$.providerRuntimeLifecycle.providerState'
                              )
                            )
                            OR (
                              delivery.terminal_event_type = 'turn.aborted'
                              AND delivery.terminal_provider_state IS NULL
                              AND json_type(
                                source.metadata_json,
                                '$.providerRuntimeLifecycle.providerState'
                              ) IS NULL
                            )
                          )
                        )
                      )
                    )
                  )
              )
            )
          )
        ORDER BY source.sequence
        LIMIT 1
      `;
      if (invalidRelationalSource.length !== 0) {
        return yield* Effect.die(new Error("migration 060 rejected verification source authority"));
      }
      afterSequence = pageLastSequence;
    }

    const contractColumns = yield* sql<{ readonly name: string }>`
      SELECT name FROM pragma_table_info('agent_control_verification_handoff_intents', 'main')
      WHERE name = 'prompt_template_version'
    `;
    if (contractColumns.length === 0) {
      const triggerRows = yield* sql<{ readonly sql: string }>`
        SELECT sql FROM main.sqlite_schema
        WHERE type = 'trigger' AND name = 'agent_control_verification_handoff_intent_validate'
          AND sql IS NOT NULL
      `;
      if (triggerRows.length !== 1) {
        return yield* Effect.die(new Error("migration 060 could not capture handoff validation"));
      }
      const originalTrigger = triggerRows[0]!.sql;
      const expandedTrigger = originalTrigger
        .replace(
          "CREATE TRIGGER agent_control_verification_handoff_intent_validate",
          "CREATE TRIGGER main.agent_control_verification_handoff_intent_validate",
        )
        .replace(
          "AND NEW.template_version IS 'agent-control-verification-prompt-v1'",
          `AND NEW.template_version IS 'agent-control-verification-prompt-v1'
        AND (${resultContractPredicate()})`,
        );
      if (expandedTrigger === originalTrigger) {
        return yield* Effect.die(new Error("migration 060 could not expand handoff validation"));
      }

      yield* injectFault("before-copy");

      yield* sql`
        ALTER TABLE main.agent_control_verification_handoff_intents
        ADD COLUMN prompt_template_version TEXT CHECK (
          prompt_template_version IS NULL
          OR prompt_template_version = 'agent-control-verification-prompt-v2'
        )
      `;
      yield* sql`
        ALTER TABLE main.agent_control_verification_handoff_intents
        ADD COLUMN prompt_contract_fingerprint TEXT
      `;
      yield* sql`
        ALTER TABLE main.agent_control_verification_handoff_intents
        ADD COLUMN result_schema_version TEXT
      `;
      yield* sql`
        ALTER TABLE main.agent_control_verification_handoff_intents
        ADD COLUMN result_schema_fingerprint TEXT
      `;

      yield* sql`DROP TRIGGER main.agent_control_verification_handoff_intent_validate`;
      yield* sql.unsafe(expandedTrigger).unprepared;
      yield* sql.unsafe(`
        CREATE TRIGGER main.agent_control_verification_handoff_result_contract_storage_validate
        BEFORE INSERT ON agent_control_verification_handoff_intents
        WHEN NOT COALESCE((${resultContractPredicate()}), 0)
        BEGIN SELECT RAISE(ABORT, 'invalid verification result contract storage'); END
      `).unprepared;
      yield* sql.unsafe(`
        CREATE TRIGGER main.agent_control_verification_handoff_result_contract_update_storage_validate
        BEFORE UPDATE ON agent_control_verification_handoff_intents
        WHEN NOT COALESCE((${resultContractPredicate()}), 0)
        BEGIN SELECT RAISE(ABORT, 'invalid verification result contract storage'); END
      `).unprepared;
    }
    yield* injectFault("after-copy");

    yield* sql`
      CREATE TABLE main.agent_control_verification_evaluation_evidence (
        evaluation_id TEXT PRIMARY KEY,
        evidence_id TEXT NOT NULL UNIQUE,
        revision INTEGER NOT NULL CHECK (revision = 1),
        evaluation_fingerprint TEXT NOT NULL UNIQUE,
        authority_digest TEXT NOT NULL,
        authority_json TEXT NOT NULL CHECK (json_valid(authority_json) = 1),
        disposition TEXT NOT NULL CHECK (disposition IN ('evaluated', 'invalid-output')),
        verdict TEXT CHECK (verdict IS NULL OR verdict IN ('passed', 'failed')),
        error_code TEXT CHECK (error_code IS NULL OR error_code IN (
          'missing-final-message', 'output-too-large', 'invalid-utf8',
          'malformed-json', 'unsupported-schema-version', 'schema-violation'
        )),
        source_disposition TEXT NOT NULL CHECK (
          source_disposition IN ('captured', 'missing', 'oversize')
        ),
        project_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
        github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
        source_identity_fingerprint TEXT NOT NULL,
        worktree_reservation_id TEXT NOT NULL UNIQUE,
        worktree_revision INTEGER NOT NULL CHECK (worktree_revision >= 1),
        worktree_event_id TEXT NOT NULL UNIQUE,
        worktree_event_sequence INTEGER NOT NULL CHECK (worktree_event_sequence >= 1),
        worktree_event_stream_version INTEGER NOT NULL CHECK (
          worktree_event_stream_version >= 1
        ),
        worktree_ownership_fingerprint TEXT NOT NULL,
        worktree_verified_at TEXT NOT NULL,
        worktree_path TEXT NOT NULL,
        branch TEXT NOT NULL,
        stage_run_id TEXT NOT NULL UNIQUE,
        attempt_id TEXT NOT NULL UNIQUE,
        lease_id TEXT NOT NULL,
        lease_holder_id TEXT NOT NULL,
        fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
        controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
        handoff_id TEXT NOT NULL UNIQUE,
        handoff_fingerprint TEXT NOT NULL UNIQUE,
        provider_delivery_id TEXT NOT NULL UNIQUE,
        thread_id TEXT NOT NULL UNIQUE,
        provider_instance_id TEXT NOT NULL,
        provider_turn_id TEXT NOT NULL,
        model_selection_fingerprint TEXT NOT NULL,
        prompt_template_version TEXT NOT NULL CHECK (
          prompt_template_version = 'agent-control-verification-prompt-v2'
        ),
        prompt_contract_fingerprint TEXT NOT NULL,
        prompt_digest TEXT NOT NULL,
        result_schema_version TEXT NOT NULL CHECK (
          result_schema_version = 'agent-control-verification-result-v1'
        ),
        result_schema_fingerprint TEXT NOT NULL,
        terminal_event_id TEXT NOT NULL UNIQUE,
        terminal_sequence INTEGER NOT NULL UNIQUE CHECK (terminal_sequence >= 1),
        terminal_stream_version INTEGER NOT NULL CHECK (terminal_stream_version >= 1),
        terminal_state TEXT NOT NULL CHECK (terminal_state = 'completed'),
        terminal_observation_digest TEXT NOT NULL,
        source_message_id TEXT,
        source_event_id TEXT,
        source_event_sequence INTEGER,
        source_event_stream_version INTEGER,
        raw_output_digest TEXT,
        output_byte_length INTEGER NOT NULL CHECK (
          output_byte_length BETWEEN 0 AND 65537
        ),
        semantic_result_digest TEXT,
        start_marker_id TEXT NOT NULL UNIQUE,
        evaluated_at TEXT NOT NULL,
        receipt_id TEXT NOT NULL UNIQUE,
        marker_id TEXT NOT NULL UNIQUE,
        CHECK (
          (source_disposition = 'missing' AND source_message_id IS NULL
            AND source_event_id IS NULL AND source_event_sequence IS NULL
            AND source_event_stream_version IS NULL AND raw_output_digest IS NULL
            AND output_byte_length = 0)
          OR (source_disposition = 'captured'
            AND source_message_id IS NOT NULL AND source_event_id IS NOT NULL
            AND source_event_sequence IS NOT NULL AND source_event_stream_version IS NOT NULL
            AND raw_output_digest IS NOT NULL AND output_byte_length <= 65536)
          OR (source_disposition = 'oversize'
            AND source_message_id IS NOT NULL AND source_event_id IS NOT NULL
            AND source_event_sequence IS NOT NULL AND source_event_stream_version IS NOT NULL
            AND raw_output_digest IS NULL AND output_byte_length = 65537)
        ),
        CHECK (
          (disposition = 'evaluated' AND verdict IS NOT NULL AND error_code IS NULL
            AND source_disposition = 'captured' AND semantic_result_digest IS NOT NULL)
          OR (disposition = 'invalid-output' AND verdict IS NULL AND error_code IS NOT NULL
            AND semantic_result_digest IS NULL)
        ),
        FOREIGN KEY (handoff_id)
          REFERENCES agent_control_verification_handoff_accepted(handoff_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (provider_delivery_id)
          REFERENCES agent_control_verification_deliveries(provider_delivery_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (start_marker_id)
          REFERENCES agent_control_verification_stage_started_markers(start_marker_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (terminal_event_id)
          REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (source_event_id)
          REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (receipt_id)
          REFERENCES agent_control_verification_evaluation_receipts(receipt_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
        FOREIGN KEY (marker_id)
          REFERENCES agent_control_verification_evaluation_markers(marker_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
      )
    `;
    yield* sql`
      CREATE TABLE main.agent_control_verification_evaluation_receipts (
        receipt_id TEXT PRIMARY KEY,
        evaluation_id TEXT NOT NULL UNIQUE,
        evidence_id TEXT NOT NULL UNIQUE,
        marker_id TEXT NOT NULL UNIQUE,
        evaluation_fingerprint TEXT NOT NULL UNIQUE,
        provider_delivery_id TEXT NOT NULL UNIQUE,
        provider_instance_id TEXT NOT NULL,
        provider_turn_id TEXT NOT NULL,
        terminal_event_id TEXT NOT NULL UNIQUE,
        terminal_observation_digest TEXT NOT NULL,
        source_message_id TEXT,
        source_event_id TEXT,
        raw_output_digest TEXT,
        output_byte_length INTEGER NOT NULL CHECK (
          output_byte_length BETWEEN 0 AND 65537
        ),
        source_disposition TEXT NOT NULL CHECK (
          source_disposition IN ('captured', 'missing', 'oversize')
        ),
        disposition TEXT NOT NULL CHECK (disposition IN ('evaluated', 'invalid-output')),
        verdict TEXT CHECK (verdict IS NULL OR verdict IN ('passed', 'failed')),
        error_code TEXT CHECK (error_code IS NULL OR error_code IN (
          'missing-final-message', 'output-too-large', 'invalid-utf8',
          'malformed-json', 'unsupported-schema-version', 'schema-violation'
        )),
        status TEXT NOT NULL CHECK (status = 'accepted'),
        accepted_at TEXT NOT NULL,
        FOREIGN KEY (evaluation_id)
          REFERENCES agent_control_verification_evaluation_evidence(evaluation_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (evidence_id)
          REFERENCES agent_control_verification_evaluation_evidence(evidence_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (marker_id)
          REFERENCES agent_control_verification_evaluation_markers(marker_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
      )
    `;
    yield* sql`
      CREATE TABLE main.agent_control_verification_evaluation_markers (
        marker_id TEXT PRIMARY KEY,
        evaluation_id TEXT NOT NULL UNIQUE,
        evidence_id TEXT NOT NULL UNIQUE,
        receipt_id TEXT NOT NULL UNIQUE,
        evaluation_fingerprint TEXT NOT NULL UNIQUE,
        provider_delivery_id TEXT NOT NULL UNIQUE,
        marker_version INTEGER NOT NULL CHECK (marker_version = 1),
        committed_at TEXT NOT NULL,
        FOREIGN KEY (evaluation_id)
          REFERENCES agent_control_verification_evaluation_evidence(evaluation_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (evidence_id)
          REFERENCES agent_control_verification_evaluation_evidence(evidence_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (receipt_id)
          REFERENCES agent_control_verification_evaluation_receipts(receipt_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT
      )
    `;
    yield* sql`
      CREATE UNIQUE INDEX main.idx_agent_control_verification_evaluation_provider_turn
      ON agent_control_verification_evaluation_evidence(provider_instance_id, provider_turn_id)
    `;
    yield* sql`
      CREATE INDEX main.idx_agent_control_verification_evaluation_candidate
      ON agent_control_verification_handoff_intents(prompt_template_version, handoff_id)
      WHERE prompt_template_version = 'agent-control-verification-prompt-v2'
    `;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_evaluation_evidence_storage_validate
      BEFORE INSERT ON agent_control_verification_evaluation_evidence
      WHEN NOT COALESCE((
        ${evidenceStorage()}
        AND json_type(NEW.authority_json, '$.report') IS NULL
      ), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification evaluation evidence storage'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_evaluation_receipt_storage_validate
      BEFORE INSERT ON agent_control_verification_evaluation_receipts
      WHEN NOT COALESCE((${receiptStorage()}), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification evaluation receipt storage'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_evaluation_marker_storage_validate
      BEFORE INSERT ON agent_control_verification_evaluation_markers
      WHEN NOT COALESCE((${markerStorage()}), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification evaluation marker storage'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_orchestration_event_storage_validate
      AFTER INSERT ON orchestration_events
      WHEN NOT COALESCE((${orchestrationEventStorage()}), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid orchestration event storage'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_orchestration_event_update_storage_validate
      BEFORE UPDATE ON orchestration_events
      WHEN NOT COALESCE((${orchestrationEventStorage()}), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid orchestration event storage'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_orchestration_message_structure_validate
      BEFORE INSERT ON orchestration_events
      WHEN typeof(NEW.event_type) = 'text'
        AND NEW.event_type = 'thread.message-sent'
        AND NOT COALESCE((
          json_type(NEW.payload_json) = 'object'
          AND (SELECT count(*) FROM json_each(NEW.payload_json)) IN (8, 9)
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.payload_json)
            WHERE key NOT IN (
              'threadId', 'messageId', 'role', 'text', 'attachments', 'turnId',
              'streaming', 'createdAt', 'updatedAt'
            )
          )
          AND ${text("json_extract(NEW.payload_json, '$.threadId')")}
          AND json_extract(NEW.payload_json, '$.threadId') IS NEW.stream_id
          AND ${text("json_extract(NEW.payload_json, '$.messageId')")}
          AND json_type(NEW.payload_json, '$.role') = 'text'
          AND json_extract(NEW.payload_json, '$.role') IN ('user', 'assistant', 'system')
          AND json_type(NEW.payload_json, '$.text') = 'text'
          AND (
            json_type(NEW.payload_json, '$.turnId') = 'null'
            OR ${text("json_extract(NEW.payload_json, '$.turnId')")}
          )
          AND json_type(NEW.payload_json, '$.streaming') IN ('true', 'false')
          AND ${timestamp("json_extract(NEW.payload_json, '$.createdAt')")}
          AND ${timestamp("json_extract(NEW.payload_json, '$.updatedAt')")}
          AND (
            json_type(NEW.payload_json, '$.attachments') IS NULL
            OR json_type(NEW.payload_json, '$.attachments') = 'array'
          )
          AND (
            json_type(NEW.metadata_json, '$.providerRuntimeMessage') IS NULL
            OR (
              json_type(NEW.metadata_json, '$.providerRuntimeMessage') = 'object'
              AND (
                SELECT count(*) FROM json_each(
                  NEW.metadata_json, '$.providerRuntimeMessage'
                )
              ) = 5
              AND (
                SELECT count(DISTINCT key) FROM json_each(
                  NEW.metadata_json, '$.providerRuntimeMessage'
                )
              ) = 5
              AND NOT EXISTS (
                SELECT 1 FROM json_each(
                  NEW.metadata_json, '$.providerRuntimeMessage'
                ) WHERE key NOT IN (
                  'runtimeEventId', 'eventType', 'providerInstanceId', 'providerTurnId',
                  'providerItemId'
                )
              )
              AND ${canonicalCorrelationId(
                "json_extract(NEW.metadata_json, '$.providerRuntimeMessage.runtimeEventId')",
              )}
              AND json_type(
                NEW.metadata_json, '$.providerRuntimeMessage.eventType'
              ) = 'text'
              AND json_extract(
                NEW.metadata_json, '$.providerRuntimeMessage.eventType'
              ) IN (
                'content.delta', 'item.completed', 'request.opened',
                'user-input.requested', 'turn.completed'
              )
              AND ${canonicalProviderInstanceId(
                "json_extract(NEW.metadata_json, '$.providerRuntimeMessage.providerInstanceId')",
              )}
              AND ${canonicalCorrelationId(
                "json_extract(NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId')",
              )}
              AND (
                json_type(
                  NEW.metadata_json, '$.providerRuntimeMessage.providerItemId'
                ) = 'null'
                OR ${canonicalCorrelationId(
                  "json_extract(NEW.metadata_json, '$.providerRuntimeMessage.providerItemId')",
                )}
              )
              AND json_extract(NEW.payload_json, '$.turnId') IS json_extract(
                NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId'
              )
              AND NEW.actor_kind = 'provider'
              AND NEW.causation_event_id IS NULL
              AND NEW.correlation_id IS NEW.command_id
            )
          )
          AND (
            json_type(NEW.metadata_json, '$.verificationResultCapture') IS NULL
            OR (
              json_type(NEW.metadata_json, '$.verificationResultCapture') = 'object'
              AND (
                SELECT count(*) FROM json_each(
                  NEW.metadata_json, '$.verificationResultCapture'
                )
              ) = 7
              AND NOT EXISTS (
                SELECT 1 FROM json_each(
                  NEW.metadata_json, '$.verificationResultCapture'
                ) WHERE key NOT IN (
                  'schemaVersion', 'disposition', 'handoffId', 'providerDeliveryId',
                  'providerInstanceId', 'providerTurnId', 'resultSchemaFingerprint'
                )
              )
              AND json_type(
                NEW.metadata_json, '$.verificationResultCapture.schemaVersion'
              ) = 'integer'
              AND json_extract(
                NEW.metadata_json, '$.verificationResultCapture.schemaVersion'
              ) = 1
              AND json_type(
                NEW.metadata_json, '$.verificationResultCapture.disposition'
              ) = 'text'
              AND json_extract(
                NEW.metadata_json, '$.verificationResultCapture.disposition'
              ) = 'presentation'
              AND ${text(
                "json_extract(NEW.metadata_json, '$.verificationResultCapture.handoffId')",
              )}
              AND ${text(
                "json_extract(NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId')",
              )}
              AND ${text(
                "json_extract(NEW.metadata_json, '$.verificationResultCapture.providerInstanceId')",
              )}
              AND ${text(
                "json_extract(NEW.metadata_json, '$.verificationResultCapture.providerTurnId')",
              )}
              AND ${sha256(
                "json_extract(NEW.metadata_json, '$.verificationResultCapture.resultSchemaFingerprint')",
              )}
              AND json_type(NEW.metadata_json, '$.providerRuntimeMessage') = 'object'
              AND json_extract(
                NEW.metadata_json, '$.verificationResultCapture.providerInstanceId'
              ) IS json_extract(
                NEW.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
              )
              AND json_extract(
                NEW.metadata_json, '$.verificationResultCapture.providerTurnId'
              ) IS json_extract(
                NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId'
              )
            )
          )
        ), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid orchestration message structure'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_result_source_seal_validate
      BEFORE INSERT ON orchestration_events
      WHEN typeof(NEW.event_type) = 'text'
        AND NEW.event_type = 'thread.session-set'
        AND json_type(NEW.metadata_json, '$.verificationResultSource') = 'object'
        AND NOT COALESCE((
          (SELECT count(*) FROM json_each(
            NEW.metadata_json, '$.verificationResultSource'
          )) = 11
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.metadata_json, '$.verificationResultSource')
            WHERE key NOT IN (
              'schemaVersion', 'handoffId', 'providerDeliveryId', 'providerInstanceId',
              'providerTurnId', 'resultSchemaFingerprint', 'sourceDisposition',
              'finalMessageId', 'sourceEventId', 'outputDigest', 'outputByteLength'
            )
          )
          AND json_type(
            NEW.metadata_json, '$.verificationResultSource.schemaVersion'
          ) = 'integer'
          AND json_extract(NEW.metadata_json, '$.verificationResultSource.schemaVersion') = 1
          AND ${text("json_extract(NEW.metadata_json, '$.verificationResultSource.handoffId')")}
          AND ${text(
            "json_extract(NEW.metadata_json, '$.verificationResultSource.providerDeliveryId')",
          )}
          AND ${text(
            "json_extract(NEW.metadata_json, '$.verificationResultSource.providerInstanceId')",
          )}
          AND ${text(
            "json_extract(NEW.metadata_json, '$.verificationResultSource.providerTurnId')",
          )}
          AND ${sha256(
            "json_extract(NEW.metadata_json, '$.verificationResultSource.resultSchemaFingerprint')",
          )}
          AND json_type(
            NEW.metadata_json, '$.verificationResultSource.sourceDisposition'
          ) = 'text'
          AND json_extract(
            NEW.metadata_json, '$.verificationResultSource.sourceDisposition'
          ) IN ('captured', 'missing', 'oversize')
          AND json_type(
            NEW.metadata_json, '$.verificationResultSource.outputByteLength'
          ) = 'integer'
          AND (SELECT count(*) FROM json_each(NEW.metadata_json)) = 2
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.metadata_json)
            WHERE key NOT IN ('providerRuntimeLifecycle', 'verificationResultSource')
          )
          AND json_type(NEW.metadata_json, '$.providerRuntimeLifecycle') = 'object'
          AND (
            SELECT count(*) FROM json_each(
              NEW.metadata_json, '$.providerRuntimeLifecycle'
            )
          ) = 5
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.metadata_json, '$.providerRuntimeLifecycle')
            WHERE key NOT IN (
              'runtimeEventId', 'runtimeEventType', 'providerInstanceId',
              'providerTurnId', 'providerState'
            )
          )
          AND ${text(
            "json_extract(NEW.metadata_json, '$.providerRuntimeLifecycle.runtimeEventId')",
          )}
          AND json_type(
            NEW.metadata_json, '$.providerRuntimeLifecycle.runtimeEventType'
          ) = 'text'
          AND json_extract(
            NEW.metadata_json, '$.providerRuntimeLifecycle.runtimeEventType'
          ) = 'turn.completed'
          AND ${text(
            "json_extract(NEW.metadata_json, '$.providerRuntimeLifecycle.providerInstanceId')",
          )}
          AND ${text(
            "json_extract(NEW.metadata_json, '$.providerRuntimeLifecycle.providerTurnId')",
          )}
          AND json_type(
            NEW.metadata_json, '$.providerRuntimeLifecycle.providerState'
          ) = 'text'
          AND json_extract(
            NEW.metadata_json, '$.providerRuntimeLifecycle.providerState'
          ) = 'completed'
          AND json_extract(
            NEW.metadata_json, '$.providerRuntimeLifecycle.providerInstanceId'
          ) IS json_extract(
            NEW.metadata_json, '$.verificationResultSource.providerInstanceId'
          )
          AND json_extract(
            NEW.metadata_json, '$.providerRuntimeLifecycle.providerTurnId'
          ) IS json_extract(
            NEW.metadata_json, '$.verificationResultSource.providerTurnId'
          )
          AND NEW.aggregate_kind = 'thread'
          AND NEW.actor_kind = 'provider'
          AND NEW.causation_event_id IS NULL
          AND NEW.command_id IS NOT NULL
          AND NEW.correlation_id IS NEW.command_id
          AND ${verificationSealCommandId()}
          AND json_type(NEW.payload_json) = 'object'
          AND (SELECT count(*) FROM json_each(NEW.payload_json)) = 2
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.payload_json)
            WHERE key NOT IN ('threadId', 'session')
          )
          AND ${text("json_extract(NEW.payload_json, '$.threadId')")}
          AND json_extract(NEW.payload_json, '$.threadId') IS NEW.stream_id
          AND json_type(NEW.payload_json, '$.session') = 'object'
          AND (SELECT count(*) FROM json_each(NEW.payload_json, '$.session')) = 8
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.payload_json, '$.session')
            WHERE key NOT IN (
              'threadId', 'status', 'providerName', 'providerInstanceId',
              'runtimeMode', 'activeTurnId', 'lastError', 'updatedAt'
            )
          )
          AND json_extract(NEW.payload_json, '$.session.threadId') IS NEW.stream_id
          AND json_type(NEW.payload_json, '$.session.status') = 'text'
          AND json_extract(NEW.payload_json, '$.session.status') = 'ready'
          AND ${text("json_extract(NEW.payload_json, '$.session.providerName')")}
          AND json_extract(NEW.payload_json, '$.session.providerInstanceId') IS json_extract(
            NEW.metadata_json, '$.verificationResultSource.providerInstanceId'
          )
          AND ${text("json_extract(NEW.payload_json, '$.session.runtimeMode')")}
          AND json_type(NEW.payload_json, '$.session.activeTurnId') = 'null'
          AND json_type(NEW.payload_json, '$.session.lastError') = 'null'
          AND ${timestamp("json_extract(NEW.payload_json, '$.session.updatedAt')")}
          AND json_extract(NEW.payload_json, '$.session.updatedAt') IS NEW.occurred_at
          AND EXISTS (
            SELECT 1
            FROM main.agent_control_verification_deliveries delivery
            JOIN main.agent_control_verification_handoff_intents intent
              ON intent.handoff_id = delivery.handoff_id
            JOIN main.agent_control_verification_handoff_accepted accepted
              ON accepted.handoff_id = intent.handoff_id
            WHERE delivery.provider_delivery_id IS json_extract(
                NEW.metadata_json, '$.verificationResultSource.providerDeliveryId'
              )
              AND delivery.handoff_id IS json_extract(
                NEW.metadata_json, '$.verificationResultSource.handoffId'
              )
              AND intent.provider_delivery_id IS delivery.provider_delivery_id
              AND accepted.provider_delivery_id IS delivery.provider_delivery_id
              AND accepted.handoff_fingerprint IS delivery.handoff_fingerprint
              AND intent.handoff_fingerprint IS delivery.handoff_fingerprint
              AND intent.thread_id IS NEW.stream_id
              AND accepted.thread_id IS NEW.stream_id
              AND delivery.thread_id IS NEW.stream_id
              AND intent.provider_instance_id IS delivery.provider_instance_id
              AND delivery.provider_instance_id IS json_extract(
                NEW.metadata_json, '$.verificationResultSource.providerInstanceId'
              )
              AND delivery.provider_turn_id IS json_extract(
                NEW.metadata_json, '$.verificationResultSource.providerTurnId'
              )
              AND intent.prompt_template_version =
                'agent-control-verification-prompt-v2'
              AND ${sha256("intent.prompt_contract_fingerprint")}
              AND intent.prompt_contract_fingerprint =
                '${AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT}'
              AND intent.result_schema_version = 'agent-control-verification-result-v1'
              AND intent.result_schema_fingerprint =
                '${AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT}'
              AND intent.result_schema_fingerprint IS json_extract(
                NEW.metadata_json, '$.verificationResultSource.resultSchemaFingerprint'
              )
              AND delivery.runtime_mode IS json_extract(
                NEW.payload_json, '$.session.runtimeMode'
              )
              AND (
                (
                  delivery.state = 'provider-started'
                  AND delivery.terminal_event_id IS NULL
                  AND delivery.terminal_event_type IS NULL
                  AND delivery.terminal_provider_state IS NULL
                  AND delivery.terminal_observation_digest IS NULL
                  AND delivery.terminal_at IS NULL
                )
                OR (
                  delivery.state = 'completed'
                  AND delivery.terminal_event_id IS json_extract(
                    NEW.metadata_json, '$.providerRuntimeLifecycle.runtimeEventId'
                  )
                  AND delivery.terminal_event_type = 'turn.completed'
                  AND delivery.terminal_provider_state = 'completed'
                  AND ${sha256("delivery.terminal_observation_digest")}
                  AND delivery.terminal_at IS NEW.occurred_at
                  AND delivery.last_error_code IS NULL
                )
              )
          )
          AND NOT EXISTS (
            SELECT 1 FROM main.orchestration_events prior_seal
            WHERE prior_seal.stream_id IS NEW.stream_id
              AND prior_seal.event_type = 'thread.session-set'
              AND json_type(
                prior_seal.metadata_json, '$.verificationResultSource'
              ) = 'object'
              AND prior_seal.event_id IS NOT NEW.event_id
          )
          AND (
            (
              json_extract(
                NEW.metadata_json, '$.verificationResultSource.sourceDisposition'
              ) = 'missing'
              AND json_type(
                NEW.metadata_json, '$.verificationResultSource.finalMessageId'
              ) = 'null'
              AND json_type(
                NEW.metadata_json, '$.verificationResultSource.sourceEventId'
              ) = 'null'
              AND json_type(
                NEW.metadata_json, '$.verificationResultSource.outputDigest'
              ) = 'null'
              AND json_extract(
                NEW.metadata_json, '$.verificationResultSource.outputByteLength'
              ) = 0
              AND NOT EXISTS (
                SELECT 1 FROM main.orchestration_events source
                WHERE source.stream_id IS NEW.stream_id
                  AND source.stream_version < NEW.stream_version
                  AND CAST(source.event_type AS BLOB) IN (
                    CAST('thread.message-sent' AS BLOB),
                    CAST('thread.verification-result-fragment-captured' AS BLOB)
                  )
                  AND (
                    (
                      CAST(source.event_type AS BLOB) =
                        CAST('thread.verification-result-fragment-captured' AS BLOB)
                      AND json_extract(source.payload_json, '$.fragment.kind') IN (
                        'delta', 'completion'
                      )
                      AND json_extract(
                        source.metadata_json,
                        '$.verificationResultCapture.disposition'
                      ) = 'authority'
                      AND json_extract(
                        source.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
                      ) IS json_extract(
                        NEW.metadata_json, '$.verificationResultSource.providerInstanceId'
                      )
                      AND json_extract(
                        source.metadata_json, '$.providerRuntimeMessage.providerTurnId'
                      ) IS json_extract(
                        NEW.metadata_json, '$.verificationResultSource.providerTurnId'
                      )
                      AND json_extract(source.payload_json, '$.threadId') IS NEW.stream_id
                      AND json_extract(source.payload_json, '$.turnId') IS json_extract(
                        NEW.metadata_json, '$.verificationResultSource.providerTurnId'
                      )
                      AND json_extract(
                        source.metadata_json, '$.verificationResultCapture.handoffId'
                      ) IS json_extract(
                        NEW.metadata_json, '$.verificationResultSource.handoffId'
                      )
                      AND json_extract(
                        source.metadata_json,
                        '$.verificationResultCapture.providerDeliveryId'
                      ) IS json_extract(
                        NEW.metadata_json, '$.verificationResultSource.providerDeliveryId'
                      )
                      AND json_extract(
                        source.metadata_json,
                        '$.verificationResultCapture.providerInstanceId'
                      ) IS json_extract(
                        NEW.metadata_json, '$.verificationResultSource.providerInstanceId'
                      )
                      AND json_extract(
                        source.metadata_json, '$.verificationResultCapture.providerTurnId'
                      ) IS json_extract(
                        NEW.metadata_json, '$.verificationResultSource.providerTurnId'
                      )
                      AND json_extract(
                        source.metadata_json,
                        '$.verificationResultCapture.resultSchemaFingerprint'
                      ) IS json_extract(
                        NEW.metadata_json,
                        '$.verificationResultSource.resultSchemaFingerprint'
                      )
                    )
                    OR (
                      CAST(source.event_type AS BLOB) = CAST('thread.message-sent' AS BLOB)
                      AND json_extract(source.payload_json, '$.role') = 'assistant'
                      AND json_extract(source.payload_json, '$.threadId') IS NEW.stream_id
                      AND json_extract(source.payload_json, '$.turnId') IS json_extract(
                        NEW.metadata_json, '$.verificationResultSource.providerTurnId'
                      )
                      AND json_extract(
                        source.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
                      ) IS json_extract(
                        NEW.metadata_json, '$.verificationResultSource.providerInstanceId'
                      )
                      AND json_extract(
                        source.metadata_json, '$.providerRuntimeMessage.providerTurnId'
                      ) IS json_extract(
                        NEW.metadata_json, '$.verificationResultSource.providerTurnId'
                      )
                      AND (
                        json_type(
                          source.metadata_json, '$.verificationResultCapture'
                        ) IS NULL
                        OR (
                          json_extract(
                            source.metadata_json, '$.verificationResultCapture.disposition'
                          ) = 'presentation'
                          AND json_extract(
                            source.metadata_json, '$.verificationResultCapture.handoffId'
                          ) IS json_extract(
                            NEW.metadata_json, '$.verificationResultSource.handoffId'
                          )
                          AND json_extract(
                            source.metadata_json,
                            '$.verificationResultCapture.providerDeliveryId'
                          ) IS json_extract(
                            NEW.metadata_json, '$.verificationResultSource.providerDeliveryId'
                          )
                          AND json_extract(
                            source.metadata_json,
                            '$.verificationResultCapture.providerInstanceId'
                          ) IS json_extract(
                            NEW.metadata_json, '$.verificationResultSource.providerInstanceId'
                          )
                          AND json_extract(
                            source.metadata_json, '$.verificationResultCapture.providerTurnId'
                          ) IS json_extract(
                            NEW.metadata_json, '$.verificationResultSource.providerTurnId'
                          )
                          AND json_extract(
                            source.metadata_json,
                            '$.verificationResultCapture.resultSchemaFingerprint'
                          ) IS json_extract(
                            NEW.metadata_json,
                            '$.verificationResultSource.resultSchemaFingerprint'
                          )
                        )
                      )
                    )
                  )
              )
            )
            OR (
              json_extract(
                NEW.metadata_json, '$.verificationResultSource.sourceDisposition'
              ) IN ('captured', 'oversize')
              AND ${text(
                "json_extract(NEW.metadata_json, '$.verificationResultSource.finalMessageId')",
              )}
              AND ${text(
                "json_extract(NEW.metadata_json, '$.verificationResultSource.sourceEventId')",
              )}
              AND (
                (
                  json_extract(
                    NEW.metadata_json, '$.verificationResultSource.sourceDisposition'
                  ) = 'captured'
                  AND ${sha256(
                    "json_extract(NEW.metadata_json, '$.verificationResultSource.outputDigest')",
                  )}
                  AND json_extract(
                    NEW.metadata_json, '$.verificationResultSource.outputByteLength'
                  ) BETWEEN 0 AND 65536
                )
                OR (
                  json_extract(
                    NEW.metadata_json, '$.verificationResultSource.sourceDisposition'
                  ) = 'oversize'
                  AND json_type(
                    NEW.metadata_json, '$.verificationResultSource.outputDigest'
                  ) = 'null'
                  AND json_extract(
                    NEW.metadata_json, '$.verificationResultSource.outputByteLength'
                  ) = 65537
                )
              )
              AND EXISTS (
                SELECT 1 FROM main.orchestration_events source
                WHERE source.event_id IS json_extract(
                    NEW.metadata_json, '$.verificationResultSource.sourceEventId'
                  )
                  AND source.stream_id IS NEW.stream_id
                  AND source.stream_version < NEW.stream_version
                  AND source.event_type = 'thread.verification-result-fragment-captured'
                  AND json_extract(source.payload_json, '$.fragment.kind') = 'completion'
                  AND json_extract(
                    source.payload_json, '$.fragment.outputByteLength'
                  ) IS json_extract(
                    NEW.metadata_json, '$.verificationResultSource.outputByteLength'
                  )
                  AND json_extract(source.payload_json, '$.messageId') IS json_extract(
                    NEW.metadata_json, '$.verificationResultSource.finalMessageId'
                  )
                  AND json_extract(source.payload_json, '$.turnId') IS json_extract(
                    NEW.metadata_json, '$.verificationResultSource.providerTurnId'
                  )
                  AND json_extract(
                    source.metadata_json, '$.verificationResultCapture.disposition'
                  ) = 'authority'
                  AND json_extract(
                    source.metadata_json, '$.verificationResultCapture.handoffId'
                  ) IS json_extract(
                    NEW.metadata_json, '$.verificationResultSource.handoffId'
                  )
                  AND json_extract(
                    source.metadata_json, '$.verificationResultCapture.providerDeliveryId'
                  ) IS json_extract(
                    NEW.metadata_json, '$.verificationResultSource.providerDeliveryId'
                  )
                  AND json_extract(
                    source.metadata_json, '$.verificationResultCapture.providerInstanceId'
                  ) IS json_extract(
                    NEW.metadata_json, '$.verificationResultSource.providerInstanceId'
                  )
                  AND json_extract(
                    source.metadata_json, '$.verificationResultCapture.providerTurnId'
                  ) IS json_extract(
                    NEW.metadata_json, '$.verificationResultSource.providerTurnId'
                  )
                  AND json_extract(
                    source.metadata_json, '$.verificationResultCapture.resultSchemaFingerprint'
                  ) IS json_extract(
                    NEW.metadata_json, '$.verificationResultSource.resultSchemaFingerprint'
                  )
              )
            )
          )
        ), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification result source seal'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_result_fragment_structure_validate
      BEFORE INSERT ON orchestration_events
      WHEN typeof(NEW.event_type) = 'text'
        AND NEW.event_type = 'thread.verification-result-fragment-captured'
        AND NOT COALESCE((
          (SELECT count(*) FROM json_each(NEW.payload_json)) = 5
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.payload_json)
            WHERE key NOT IN ('threadId', 'messageId', 'turnId', 'fragment', 'createdAt')
          )
          AND ${text("json_extract(NEW.payload_json, '$.threadId')")}
          AND json_extract(NEW.payload_json, '$.threadId') IS NEW.stream_id
          AND ${text("json_extract(NEW.payload_json, '$.messageId')")}
          AND ${text("json_extract(NEW.payload_json, '$.turnId')")}
          AND ${timestamp("json_extract(NEW.payload_json, '$.createdAt')")}
          AND json_type(NEW.payload_json, '$.fragment') = 'object'
          AND (SELECT count(*) FROM json_each(NEW.metadata_json)) = 2
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.metadata_json)
            WHERE key NOT IN ('providerRuntimeMessage', 'verificationResultCapture')
          )
          AND json_type(NEW.metadata_json, '$.providerRuntimeMessage') = 'object'
          AND (
            SELECT count(*) FROM json_each(NEW.metadata_json, '$.providerRuntimeMessage')
          ) = 5
          AND (
            SELECT count(DISTINCT key)
            FROM json_each(NEW.metadata_json, '$.providerRuntimeMessage')
          ) = 5
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.metadata_json, '$.providerRuntimeMessage')
            WHERE key NOT IN (
              'runtimeEventId', 'eventType', 'providerInstanceId', 'providerTurnId',
              'providerItemId'
            )
          )
          AND ${canonicalCorrelationId(
            "json_extract(NEW.metadata_json, '$.providerRuntimeMessage.runtimeEventId')",
          )}
          AND json_type(
            NEW.metadata_json, '$.providerRuntimeMessage.eventType'
          ) = 'text'
          AND json_extract(
            NEW.metadata_json, '$.providerRuntimeMessage.eventType'
          ) IN (
            'content.delta', 'item.completed', 'request.opened',
            'user-input.requested', 'turn.completed'
          )
          AND ${canonicalProviderInstanceId(
            "json_extract(NEW.metadata_json, '$.providerRuntimeMessage.providerInstanceId')",
          )}
          AND ${canonicalCorrelationId(
            "json_extract(NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId')",
          )}
          AND (
            json_type(NEW.metadata_json, '$.providerRuntimeMessage.providerItemId') = 'null'
            OR ${canonicalCorrelationId(
              "json_extract(NEW.metadata_json, '$.providerRuntimeMessage.providerItemId')",
            )}
          )
          AND json_type(NEW.metadata_json, '$.verificationResultCapture') = 'object'
          AND (
            SELECT count(*) FROM json_each(NEW.metadata_json, '$.verificationResultCapture')
          ) = 7
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.metadata_json, '$.verificationResultCapture')
            WHERE key NOT IN (
              'schemaVersion', 'disposition', 'handoffId', 'providerDeliveryId',
              'providerInstanceId', 'providerTurnId', 'resultSchemaFingerprint'
            )
          )
          AND json_type(
            NEW.metadata_json, '$.verificationResultCapture.schemaVersion'
          ) = 'integer'
          AND json_type(
            NEW.metadata_json, '$.verificationResultCapture.disposition'
          ) = 'text'
          AND ${text("json_extract(NEW.metadata_json, '$.verificationResultCapture.handoffId')")}
          AND ${text(
            "json_extract(NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId')",
          )}
          AND ${text(
            "json_extract(NEW.metadata_json, '$.verificationResultCapture.providerInstanceId')",
          )}
          AND ${text(
            "json_extract(NEW.metadata_json, '$.verificationResultCapture.providerTurnId')",
          )}
          AND ${sha256(
            "json_extract(NEW.metadata_json, '$.verificationResultCapture.resultSchemaFingerprint')",
          )}
          AND json_extract(NEW.payload_json, '$.turnId') IS json_extract(
            NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId'
          )
          AND json_extract(
            NEW.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
          ) IS json_extract(
            NEW.metadata_json, '$.verificationResultCapture.providerInstanceId'
          )
          AND json_extract(
            NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId'
          ) IS json_extract(
            NEW.metadata_json, '$.verificationResultCapture.providerTurnId'
          )
          AND NOT EXISTS (
            SELECT 1 FROM main.orchestration_events completed
            WHERE completed.stream_id IS NEW.stream_id
              AND completed.stream_version < NEW.stream_version
              AND completed.event_type = 'thread.verification-result-fragment-captured'
              AND json_extract(completed.payload_json, '$.messageId') IS json_extract(
                NEW.payload_json, '$.messageId'
              )
              AND json_extract(completed.payload_json, '$.fragment.kind') = 'completion'
              AND json_extract(
                completed.metadata_json, '$.verificationResultCapture.providerDeliveryId'
              ) IS json_extract(
                NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
              )
          )
          AND (
            NOT EXISTS (
              SELECT 1 FROM main.orchestration_events latest
              WHERE latest.stream_id IS NEW.stream_id
                AND latest.stream_version < NEW.stream_version
                AND latest.event_type = 'thread.verification-result-fragment-captured'
                AND json_extract(
                  latest.metadata_json, '$.verificationResultCapture.providerDeliveryId'
                ) IS json_extract(
                  NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
                )
            )
            OR (
              SELECT CASE
                WHEN json_extract(latest.payload_json, '$.fragment.kind') = 'delta'
                  THEN json_extract(latest.payload_json, '$.messageId') IS json_extract(
                    NEW.payload_json, '$.messageId'
                  )
                ELSE json_extract(latest.payload_json, '$.messageId') IS NOT json_extract(
                  NEW.payload_json, '$.messageId'
                )
              END
              FROM main.orchestration_events latest
              WHERE latest.stream_id IS NEW.stream_id
                AND latest.stream_version < NEW.stream_version
                AND latest.event_type = 'thread.verification-result-fragment-captured'
                AND json_extract(
                  latest.metadata_json, '$.verificationResultCapture.providerDeliveryId'
                ) IS json_extract(
                  NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
                )
              ORDER BY latest.stream_version DESC
              LIMIT 1
            )
          )
          AND (
            (
              json_extract(NEW.payload_json, '$.fragment.kind') = 'delta'
              AND (SELECT count(*) FROM json_each(NEW.payload_json, '$.fragment')) = 8
              AND (
                SELECT count(DISTINCT key) FROM json_each(NEW.payload_json, '$.fragment')
              ) = 8
              AND NOT EXISTS (
                SELECT 1 FROM json_each(NEW.payload_json, '$.fragment')
                WHERE key NOT IN (
                  'kind', 'textPrefix', 'prefixByteLength', 'fullTextByteLength',
                  'fullTextDigest', 'cumulativeSourceByteLength', 'fragmentOrdinal',
                  'cumulativeEvidenceDigest'
                )
              )
              AND json_type(NEW.payload_json, '$.fragment.textPrefix') = 'text'
              AND length(CAST(json_extract(
                NEW.payload_json, '$.fragment.textPrefix'
              ) AS BLOB)) <= 65536
              AND ${safeInteger("json_extract(NEW.payload_json, '$.fragment.prefixByteLength')")}
              AND json_extract(
                NEW.payload_json, '$.fragment.prefixByteLength'
              ) = length(CAST(json_extract(
                NEW.payload_json, '$.fragment.textPrefix'
              ) AS BLOB))
              AND json_extract(
                NEW.payload_json, '$.fragment.prefixByteLength'
              ) <= 65536
              AND ${safeInteger("json_extract(NEW.payload_json, '$.fragment.fullTextByteLength')")}
              AND json_extract(
                NEW.payload_json, '$.fragment.prefixByteLength'
              ) <= json_extract(
                NEW.payload_json, '$.fragment.fullTextByteLength'
              )
              AND ${sha256("json_extract(NEW.payload_json, '$.fragment.fullTextDigest')")}
              AND ${safeInteger(
                "json_extract(NEW.payload_json, '$.fragment.cumulativeSourceByteLength')",
              )}
              AND json_extract(
                NEW.payload_json, '$.fragment.cumulativeSourceByteLength'
              ) = CASE
                WHEN ${previousVerificationCaptureByteLength} >= 65537
                  OR json_extract(NEW.payload_json, '$.fragment.fullTextByteLength')
                    > 65537 - ${previousVerificationCaptureByteLength}
                  THEN 65537
                ELSE ${previousVerificationCaptureByteLength}
                  + json_extract(NEW.payload_json, '$.fragment.fullTextByteLength')
              END
              AND json_extract(
                NEW.payload_json, '$.fragment.prefixByteLength'
              ) <= 65536 - ${previousVerificationCaptureStoredByteLength}
              AND (
                (
                  json_extract(NEW.payload_json, '$.fragment.fullTextByteLength')
                    <= 65536 - ${previousVerificationCaptureStoredByteLength}
                  AND json_extract(
                    NEW.payload_json, '$.fragment.prefixByteLength'
                  ) = json_extract(
                    NEW.payload_json, '$.fragment.fullTextByteLength'
                  )
                  AND t3_verification_delta_digest(json_extract(
                    NEW.payload_json, '$.fragment.textPrefix'
                  )) IS json_extract(
                    NEW.payload_json, '$.fragment.fullTextDigest'
                  )
                )
                OR (
                  json_extract(NEW.payload_json, '$.fragment.fullTextByteLength')
                    > 65536 - ${previousVerificationCaptureStoredByteLength}
                  AND 65536 - ${previousVerificationCaptureStoredByteLength}
                    - json_extract(NEW.payload_json, '$.fragment.prefixByteLength') < 4
                )
              )
              AND ${safeInteger("json_extract(NEW.payload_json, '$.fragment.fragmentOrdinal')")}
              AND json_extract(NEW.payload_json, '$.fragment.fragmentOrdinal')
                = ${previousVerificationCaptureFragmentOrdinal} + 1
              AND ${sha256("json_extract(NEW.payload_json, '$.fragment.cumulativeEvidenceDigest')")}
              AND json_extract(
                NEW.payload_json, '$.fragment.cumulativeEvidenceDigest'
              ) IS t3_verification_evidence_digest(
                ${previousVerificationCaptureEvidenceDigest}, 'delta',
                json_extract(NEW.payload_json, '$.fragment.fragmentOrdinal'), 1,
                json_extract(NEW.payload_json, '$.fragment.fullTextByteLength'),
                json_extract(NEW.payload_json, '$.fragment.fullTextDigest')
              )
              AND json_extract(
                NEW.metadata_json, '$.providerRuntimeMessage.eventType'
              ) = 'content.delta'
            )
            OR (
              json_extract(NEW.payload_json, '$.fragment.kind') = 'completion'
              AND (SELECT count(*) FROM json_each(NEW.payload_json, '$.fragment')) = 6
              AND (
                SELECT count(DISTINCT key) FROM json_each(NEW.payload_json, '$.fragment')
              ) = 6
              AND NOT EXISTS (
                SELECT 1 FROM json_each(NEW.payload_json, '$.fragment')
                WHERE key NOT IN (
                  'kind', 'completionTextPrefix', 'outputByteLength', 'completionDetail',
                  'fragmentOrdinal', 'cumulativeEvidenceDigest'
                )
              )
              AND json_type(
                NEW.payload_json, '$.fragment.completionTextPrefix'
              ) IN ('null', 'text')
              AND ${safeInteger("json_extract(NEW.payload_json, '$.fragment.outputByteLength')")}
              AND json_extract(NEW.payload_json, '$.fragment.outputByteLength') <= 65537
              AND json_type(NEW.payload_json, '$.fragment.completionDetail') = 'object'
              AND (SELECT count(*) FROM json_each(
                NEW.payload_json, '$.fragment.completionDetail'
              )) IN (1, 3)
              AND (SELECT count(DISTINCT key) FROM json_each(
                NEW.payload_json, '$.fragment.completionDetail'
              )) IN (1, 3)
              AND NOT EXISTS (
                SELECT 1 FROM json_each(NEW.payload_json, '$.fragment.completionDetail')
                WHERE key NOT IN ('present', 'fullByteLength', 'fullDigest')
              )
              AND (
                (
                  json_type(
                    NEW.payload_json, '$.fragment.completionDetail.present'
                  ) = 'false'
                  AND (SELECT count(*) FROM json_each(
                    NEW.payload_json, '$.fragment.completionDetail'
                  )) = 1
                )
                OR (
                  json_type(
                    NEW.payload_json, '$.fragment.completionDetail.present'
                  ) = 'true'
                  AND (SELECT count(*) FROM json_each(
                    NEW.payload_json, '$.fragment.completionDetail'
                  )) = 3
                  AND ${safeInteger(
                    "json_extract(NEW.payload_json, '$.fragment.completionDetail.fullByteLength')",
                  )}
                  AND ${sha256(
                    "json_extract(NEW.payload_json, '$.fragment.completionDetail.fullDigest')",
                  )}
                )
              )
              AND (
                (
                  json_type(
                    NEW.payload_json, '$.fragment.completionTextPrefix'
                  ) = 'null'
                  AND json_extract(
                    NEW.payload_json, '$.fragment.outputByteLength'
                  ) = ${previousVerificationCaptureByteLength}
                  AND (
                    ${previousVerificationCaptureFragmentOrdinal} > 0
                    OR json_type(
                      NEW.payload_json, '$.fragment.completionDetail.present'
                    ) = 'false'
                  )
                )
                OR (
                  json_type(
                    NEW.payload_json, '$.fragment.completionTextPrefix'
                  ) = 'text'
                  AND ${previousVerificationCaptureFragmentOrdinal} = 0
                  AND json_type(
                    NEW.payload_json, '$.fragment.completionDetail.present'
                  ) = 'true'
                  AND length(CAST(json_extract(
                    NEW.payload_json, '$.fragment.completionTextPrefix'
                  ) AS BLOB)) <= 65536
                  AND length(CAST(json_extract(
                    NEW.payload_json, '$.fragment.completionTextPrefix'
                  ) AS BLOB)) <= json_extract(
                    NEW.payload_json, '$.fragment.completionDetail.fullByteLength'
                  )
                  AND json_extract(
                    NEW.payload_json, '$.fragment.outputByteLength'
                  ) = CASE
                    WHEN json_extract(
                      NEW.payload_json, '$.fragment.completionDetail.fullByteLength'
                    ) > 65536 THEN 65537
                    ELSE json_extract(
                      NEW.payload_json, '$.fragment.completionDetail.fullByteLength'
                    )
                  END
                  AND (
                    (
                      json_extract(
                        NEW.payload_json, '$.fragment.completionDetail.fullByteLength'
                      ) <= 65536
                      AND length(CAST(json_extract(
                        NEW.payload_json, '$.fragment.completionTextPrefix'
                      ) AS BLOB)) = json_extract(
                        NEW.payload_json, '$.fragment.completionDetail.fullByteLength'
                      )
                      AND t3_verification_completion_digest(json_extract(
                        NEW.payload_json, '$.fragment.completionTextPrefix'
                      )) IS json_extract(
                        NEW.payload_json, '$.fragment.completionDetail.fullDigest'
                      )
                    )
                    OR (
                      json_extract(
                        NEW.payload_json, '$.fragment.completionDetail.fullByteLength'
                      ) > 65536
                      AND 65536 - length(CAST(json_extract(
                        NEW.payload_json, '$.fragment.completionTextPrefix'
                      ) AS BLOB)) < 4
                    )
                  )
                )
              )
              AND ${safeInteger("json_extract(NEW.payload_json, '$.fragment.fragmentOrdinal')")}
              AND json_extract(NEW.payload_json, '$.fragment.fragmentOrdinal')
                = ${previousVerificationCaptureFragmentOrdinal} + 1
              AND ${sha256("json_extract(NEW.payload_json, '$.fragment.cumulativeEvidenceDigest')")}
              AND json_extract(
                NEW.payload_json, '$.fragment.cumulativeEvidenceDigest'
              ) IS t3_verification_evidence_digest(
                ${previousVerificationCaptureEvidenceDigest}, 'completion',
                json_extract(NEW.payload_json, '$.fragment.fragmentOrdinal'),
                CASE WHEN json_type(
                  NEW.payload_json, '$.fragment.completionDetail.present'
                ) = 'true' THEN 1 ELSE 0 END,
                CASE WHEN json_type(
                  NEW.payload_json, '$.fragment.completionDetail.present'
                ) = 'true' THEN json_extract(
                  NEW.payload_json, '$.fragment.completionDetail.fullByteLength'
                ) ELSE NULL END,
                CASE WHEN json_type(
                  NEW.payload_json, '$.fragment.completionDetail.present'
                ) = 'true' THEN json_extract(
                  NEW.payload_json, '$.fragment.completionDetail.fullDigest'
                ) ELSE NULL END
              )
              AND json_extract(
                NEW.metadata_json, '$.providerRuntimeMessage.eventType'
              ) IN ('item.completed', 'request.opened', 'user-input.requested', 'turn.completed')
            )
          )
        ), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification result fragment structure'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_result_capture_validate
      BEFORE INSERT ON orchestration_events
      WHEN NEW.event_type = 'thread.verification-result-fragment-captured'
        AND NOT COALESCE(EXISTS (
          SELECT 1
          FROM main.agent_control_verification_deliveries delivery
          JOIN main.agent_control_verification_handoff_intents intent
            ON intent.handoff_id = delivery.handoff_id
          WHERE typeof(NEW.stream_id) = 'text'
            AND typeof(NEW.event_type) = 'text'
            AND typeof(NEW.command_id) = 'text'
            AND typeof(NEW.actor_kind) = 'text'
            AND typeof(NEW.payload_json) = 'text'
            AND typeof(NEW.metadata_json) = 'text'
            AND json_valid(NEW.payload_json) = 1
            AND json_valid(NEW.metadata_json) = 1
            AND NEW.actor_kind = 'provider'
            AND json_extract(NEW.payload_json, '$.threadId') IS NEW.stream_id
            AND json_extract(NEW.payload_json, '$.turnId') IS delivery.provider_turn_id
            AND json_type(NEW.payload_json, '$.messageId') = 'text'
            AND (
              (json_extract(NEW.payload_json, '$.fragment.kind') = 'delta'
                AND json_type(NEW.payload_json, '$.fragment.textPrefix') = 'text'
                AND json_extract(
                  NEW.metadata_json, '$.providerRuntimeMessage.eventType'
                ) = 'content.delta')
              OR
              (json_extract(NEW.payload_json, '$.fragment.kind') = 'completion'
                AND json_type(NEW.payload_json, '$.fragment.text') IS NULL
                AND json_type(
                  NEW.payload_json, '$.fragment.completionTextPrefix'
                ) IN ('null', 'text')
                AND json_extract(
                  NEW.metadata_json, '$.providerRuntimeMessage.eventType'
                ) IN ('item.completed', 'request.opened', 'user-input.requested', 'turn.completed'))
            )
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.schemaVersion'
            ) = 1
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.disposition'
            ) = 'authority'
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.handoffId'
            ) IS delivery.handoff_id
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
            ) IS delivery.provider_delivery_id
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.providerInstanceId'
            ) IS delivery.provider_instance_id
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.providerTurnId'
            ) IS delivery.provider_turn_id
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.resultSchemaFingerprint'
            ) IS intent.result_schema_fingerprint
            AND json_extract(
              NEW.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
            ) IS delivery.provider_instance_id
            AND json_extract(
              NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId'
            ) IS delivery.provider_turn_id
            AND NEW.command_id IS 'provider:' || json_extract(
              NEW.metadata_json, '$.providerRuntimeMessage.runtimeEventId'
            ) || ':verification-result:' || json_extract(
              NEW.payload_json, '$.messageId'
            )
            AND intent.prompt_template_version = 'agent-control-verification-prompt-v2'
            AND delivery.thread_id IS NEW.stream_id
            AND delivery.state IN ('provider-started', 'completed')
        ), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification result capture authority'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_result_post_seal_reject
      BEFORE INSERT ON orchestration_events
      WHEN NEW.event_type IN (
        'thread.message-sent', 'thread.verification-result-fragment-captured'
      ) AND COALESCE(EXISTS (
        SELECT 1
        FROM main.orchestration_events sealed
        WHERE sealed.stream_id IS NEW.stream_id
          AND sealed.event_type = 'thread.session-set'
          AND json_type(sealed.metadata_json, '$.verificationResultSource') = 'object'
          AND (
            (
              NEW.event_type = 'thread.verification-result-fragment-captured'
              AND json_extract(
                NEW.metadata_json, '$.verificationResultCapture.handoffId'
              ) IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.handoffId'
              )
              AND json_extract(
                NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
              ) IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.providerDeliveryId'
              )
              AND json_extract(
                NEW.metadata_json, '$.verificationResultCapture.resultSchemaFingerprint'
              ) IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.resultSchemaFingerprint'
              )
              AND json_extract(
                NEW.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
              ) IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.providerInstanceId'
              )
              AND json_extract(
                NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId'
              ) IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.providerTurnId'
              )
            )
            OR (
              NEW.event_type = 'thread.message-sent'
              AND json_extract(NEW.payload_json, '$.role') = 'assistant'
              AND (
                (
                  json_extract(
                    sealed.metadata_json, '$.verificationResultSource.finalMessageId'
                  ) IS NOT NULL
                  AND json_extract(NEW.payload_json, '$.messageId') IS json_extract(
                    sealed.metadata_json, '$.verificationResultSource.finalMessageId'
                  )
                )
                OR NEW.event_id IS json_extract(
                  sealed.metadata_json, '$.verificationResultSource.sourceEventId'
                )
                OR (
                  json_extract(
                    NEW.metadata_json, '$.verificationResultCapture.handoffId'
                  ) IS json_extract(
                    sealed.metadata_json, '$.verificationResultSource.handoffId'
                  )
                  AND json_extract(
                    NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
                  ) IS json_extract(
                    sealed.metadata_json, '$.verificationResultSource.providerDeliveryId'
                  )
                  AND json_extract(
                    NEW.metadata_json, '$.verificationResultCapture.providerInstanceId'
                  ) IS json_extract(
                    sealed.metadata_json, '$.verificationResultSource.providerInstanceId'
                  )
                  AND json_extract(
                    NEW.metadata_json, '$.verificationResultCapture.providerTurnId'
                  ) IS json_extract(
                    sealed.metadata_json, '$.verificationResultSource.providerTurnId'
                  )
                )
                OR (
                  json_extract(
                    NEW.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
                  ) IS json_extract(
                    sealed.metadata_json, '$.verificationResultSource.providerInstanceId'
                  )
                  AND json_extract(
                    NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId'
                  ) IS json_extract(
                    sealed.metadata_json, '$.verificationResultSource.providerTurnId'
                  )
                )
                OR json_extract(NEW.payload_json, '$.turnId') IS json_extract(
                  sealed.metadata_json, '$.verificationResultSource.providerTurnId'
                )
              )
            )
          )
      ), 0)
      BEGIN SELECT RAISE(ABORT, 'verification result source is sealed'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_result_authority_no_update
      BEFORE UPDATE ON orchestration_events
      WHEN json_type(OLD.metadata_json, '$.verificationResultCapture') = 'object'
        OR json_type(OLD.metadata_json, '$.verificationResultSource') = 'object'
        OR json_type(NEW.metadata_json, '$.verificationResultCapture') = 'object'
        OR json_type(NEW.metadata_json, '$.verificationResultSource') = 'object'
        OR EXISTS (
          SELECT 1 FROM main.orchestration_events sealed
          WHERE json_type(sealed.metadata_json, '$.verificationResultSource') = 'object'
            AND (
              json_extract(sealed.metadata_json, '$.verificationResultSource.sourceEventId')
                IS OLD.event_id
              OR json_extract(sealed.metadata_json, '$.verificationResultSource.sourceEventId')
                IS NEW.event_id
            )
        )
      BEGIN SELECT RAISE(ABORT, 'verification result authority is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_result_authority_no_replace
      BEFORE INSERT ON orchestration_events
      WHEN EXISTS (
        SELECT 1 FROM main.orchestration_events existing
        WHERE existing.event_id IS NEW.event_id
          AND (
            json_type(existing.metadata_json, '$.verificationResultCapture') = 'object'
            OR json_type(existing.metadata_json, '$.verificationResultSource') = 'object'
            OR EXISTS (
              SELECT 1 FROM main.orchestration_events sealed
              WHERE json_type(
                  sealed.metadata_json, '$.verificationResultSource'
                ) = 'object'
                AND json_extract(
                  sealed.metadata_json, '$.verificationResultSource.sourceEventId'
                ) IS existing.event_id
            )
          )
      )
      BEGIN SELECT RAISE(ABORT, 'verification result authority is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_result_authority_no_delete
      BEFORE DELETE ON orchestration_events
      WHEN json_type(OLD.metadata_json, '$.verificationResultCapture') = 'object'
        OR json_type(OLD.metadata_json, '$.verificationResultSource') = 'object'
        OR EXISTS (
          SELECT 1 FROM main.orchestration_events sealed
          WHERE json_type(sealed.metadata_json, '$.verificationResultSource') = 'object'
            AND json_extract(
              sealed.metadata_json, '$.verificationResultSource.sourceEventId'
            ) IS OLD.event_id
        )
      BEGIN SELECT RAISE(ABORT, 'verification result authority is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE UNIQUE INDEX main.${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX}
      ON orchestration_events (
        CAST(json_extract(
          metadata_json, '$.providerRuntimeMessage.runtimeEventId'
        ) AS BLOB)
      )
      WHERE typeof(event_type) = 'text'
        AND CAST(event_type AS BLOB) =
          CAST('thread.verification-result-fragment-captured' AS BLOB)
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER}
      BEFORE INSERT ON orchestration_events
      WHEN typeof(NEW.event_type) = 'text'
        AND CAST(NEW.event_type AS BLOB) =
          CAST('thread.verification-result-fragment-captured' AS BLOB)
        AND EXISTS (
          SELECT 1
          FROM main.orchestration_events authoritative
          WHERE typeof(authoritative.event_type) = 'text'
            AND CAST(authoritative.event_type AS BLOB) =
              CAST('thread.verification-result-fragment-captured' AS BLOB)
            AND CAST(json_extract(
              authoritative.metadata_json,
              '$.providerRuntimeMessage.runtimeEventId'
            ) AS BLOB) = CAST(json_extract(
              NEW.metadata_json,
              '$.providerRuntimeMessage.runtimeEventId'
            ) AS BLOB)
        )
      BEGIN
        SELECT RAISE(ABORT, '${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_CONFLICT}');
      END
    `).unprepared;
    yield* injectFault("after-runtime-authority-install");
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_evaluation_evidence_validate
      BEFORE INSERT ON agent_control_verification_evaluation_evidence
      WHEN NOT EXISTS (
        SELECT 1
        FROM main.agent_control_verification_deliveries delivery
        JOIN main.agent_control_verification_handoff_intents intent
          ON intent.handoff_id = delivery.handoff_id
        JOIN main.agent_control_verification_stage_started_markers started
          ON started.provider_delivery_id = delivery.provider_delivery_id
        JOIN main.agent_control_verification_stage_started_evidence stage_start
          ON stage_start.start_evidence_id = started.start_evidence_id
        JOIN main.agent_control_stage_run_states stage
          ON stage.stage_run_id = delivery.stage_run_id
        JOIN main.agent_control_stage_run_lease_states lease
          ON lease.lease_id = delivery.lease_id
        JOIN main.orchestration_events terminal
          ON terminal.event_id = NEW.terminal_event_id
        LEFT JOIN main.orchestration_events source
          ON source.event_id = NEW.source_event_id
        WHERE delivery.provider_delivery_id IS NEW.provider_delivery_id
          AND delivery.handoff_id IS NEW.handoff_id
          AND delivery.handoff_fingerprint IS NEW.handoff_fingerprint
          AND delivery.thread_id IS NEW.thread_id
          AND delivery.stage_run_id IS NEW.stage_run_id
          AND delivery.attempt_id IS NEW.attempt_id
          AND delivery.lease_id IS NEW.lease_id
          AND delivery.lease_holder_id IS NEW.lease_holder_id
          AND delivery.fence_token IS NEW.fence_token
          AND delivery.controlled_thread_reservation_id IS
            NEW.controlled_thread_reservation_id
          AND delivery.provider_instance_id IS NEW.provider_instance_id
          AND delivery.provider_turn_id IS NEW.provider_turn_id
          AND delivery.model_selection_fingerprint IS NEW.model_selection_fingerprint
          AND delivery.state = 'completed'
          AND delivery.terminal_provider_state = 'completed'
          AND delivery.terminal_observation_digest IS NEW.terminal_observation_digest
          AND intent.project_id IS NEW.project_id
          AND intent.task_id IS NEW.task_id
          AND intent.task_revision IS NEW.task_revision
          AND intent.github_intake_sequence IS NEW.github_intake_sequence
          AND intent.source_identity_fingerprint IS NEW.source_identity_fingerprint
          AND intent.worktree_reservation_id IS NEW.worktree_reservation_id
          AND intent.worktree_revision IS NEW.worktree_revision
          AND intent.worktree_event_id IS NEW.worktree_event_id
          AND intent.worktree_event_sequence IS NEW.worktree_event_sequence
          AND intent.worktree_event_stream_version IS NEW.worktree_event_stream_version
          AND intent.worktree_ownership_fingerprint IS NEW.worktree_ownership_fingerprint
          AND intent.worktree_verified_at IS NEW.worktree_verified_at
          AND intent.worktree_path IS NEW.worktree_path
          AND intent.branch IS NEW.branch
          AND stage_start.handoff_id IS NEW.handoff_id
          AND stage_start.handoff_fingerprint IS NEW.handoff_fingerprint
          AND stage_start.project_id IS NEW.project_id
          AND stage_start.task_id IS NEW.task_id
          AND stage_start.task_revision IS NEW.task_revision
          AND stage_start.github_intake_sequence IS NEW.github_intake_sequence
          AND stage_start.source_identity_fingerprint IS NEW.source_identity_fingerprint
          AND stage_start.stage_run_id IS NEW.stage_run_id
          AND stage_start.attempt_id IS NEW.attempt_id
          AND stage_start.controlled_thread_reservation_id IS
            NEW.controlled_thread_reservation_id
          AND stage_start.thread_id IS NEW.thread_id
          AND stage_start.lease_id IS NEW.lease_id
          AND stage_start.lease_holder_id IS NEW.lease_holder_id
          AND stage_start.fence_token IS NEW.fence_token
          AND stage_start.provider_instance_id IS NEW.provider_instance_id
          AND stage_start.provider_turn_id IS NEW.provider_turn_id
          AND stage_start.model_selection_fingerprint IS NEW.model_selection_fingerprint
          AND intent.prompt_template_version IS NEW.prompt_template_version
          AND intent.prompt_contract_fingerprint IS NEW.prompt_contract_fingerprint
          AND intent.prompt_digest IS NEW.prompt_digest
          AND intent.result_schema_version IS NEW.result_schema_version
          AND intent.result_schema_fingerprint IS NEW.result_schema_fingerprint
          AND started.start_marker_id IS NEW.start_marker_id
          AND started.committed_at <= NEW.evaluated_at
          AND stage.status = 'running' AND stage.revision = 2
          AND lease.status = 'reserved'
          AND lease.holder_id IS NEW.lease_holder_id
          AND lease.fence_token IS NEW.fence_token
          AND terminal.sequence IS NEW.terminal_sequence
          AND terminal.stream_version IS NEW.terminal_stream_version
          AND terminal.stream_id IS NEW.thread_id
          AND terminal.event_type = 'thread.session-set'
          AND json_extract(
            terminal.metadata_json, '$.providerRuntimeLifecycle.runtimeEventId'
          ) IS delivery.terminal_event_id
          AND json_extract(
            terminal.metadata_json, '$.providerRuntimeLifecycle.providerState'
          ) = 'completed'
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.handoffId'
          ) IS NEW.handoff_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.providerDeliveryId'
          ) IS NEW.provider_delivery_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.providerInstanceId'
          ) IS NEW.provider_instance_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.providerTurnId'
          ) IS NEW.provider_turn_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.resultSchemaFingerprint'
          ) IS NEW.result_schema_fingerprint
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.sourceDisposition'
          ) IS NEW.source_disposition
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.finalMessageId'
          ) IS NEW.source_message_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.sourceEventId'
          ) IS NEW.source_event_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.outputDigest'
          ) IS NEW.raw_output_digest
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.outputByteLength'
          ) IS NEW.output_byte_length
          AND (
            (NEW.source_event_id IS NULL AND source.event_id IS NULL)
            OR (
              source.event_id IS NEW.source_event_id
              AND source.sequence IS NEW.source_event_sequence
              AND source.stream_version IS NEW.source_event_stream_version
              AND source.stream_id IS NEW.thread_id
              AND source.event_type = 'thread.verification-result-fragment-captured'
              AND json_extract(source.payload_json, '$.messageId') IS NEW.source_message_id
              AND json_extract(source.payload_json, '$.turnId') IS NEW.provider_turn_id
              AND json_extract(source.payload_json, '$.fragment.kind') = 'completion'
              AND json_extract(
                source.metadata_json, '$.verificationResultCapture.disposition'
              ) = 'authority'
              AND json_extract(
                source.metadata_json, '$.verificationResultCapture.handoffId'
              ) IS NEW.handoff_id
              AND json_extract(
                source.metadata_json, '$.verificationResultCapture.providerDeliveryId'
              ) IS NEW.provider_delivery_id
              AND json_extract(
                source.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
              ) IS NEW.provider_instance_id
              AND json_extract(
                source.metadata_json, '$.providerRuntimeMessage.providerTurnId'
              ) IS NEW.provider_turn_id
            )
          )
      )
      BEGIN SELECT RAISE(ABORT, 'verification evaluation authority is inconsistent'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_evaluation_receipt_validate
      BEFORE INSERT ON agent_control_verification_evaluation_receipts
      WHEN NOT EXISTS (
        SELECT 1 FROM main.agent_control_verification_evaluation_evidence evidence
        WHERE evidence.evaluation_id IS NEW.evaluation_id
          AND evidence.evidence_id IS NEW.evidence_id
          AND evidence.marker_id IS NEW.marker_id
          AND evidence.evaluation_fingerprint IS NEW.evaluation_fingerprint
          AND evidence.provider_delivery_id IS NEW.provider_delivery_id
          AND evidence.provider_instance_id IS NEW.provider_instance_id
          AND evidence.provider_turn_id IS NEW.provider_turn_id
          AND evidence.terminal_event_id IS NEW.terminal_event_id
          AND evidence.terminal_observation_digest IS NEW.terminal_observation_digest
          AND evidence.source_message_id IS NEW.source_message_id
          AND evidence.source_event_id IS NEW.source_event_id
          AND evidence.raw_output_digest IS NEW.raw_output_digest
          AND evidence.output_byte_length IS NEW.output_byte_length
          AND evidence.source_disposition IS NEW.source_disposition
          AND evidence.disposition IS NEW.disposition
          AND evidence.verdict IS NEW.verdict
          AND evidence.error_code IS NEW.error_code
          AND evidence.evaluated_at IS NEW.accepted_at
      )
      BEGIN SELECT RAISE(ABORT, 'verification evaluation receipt is inconsistent'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_evaluation_marker_validate
      BEFORE INSERT ON agent_control_verification_evaluation_markers
      WHEN NOT EXISTS (
        SELECT 1
        FROM main.agent_control_verification_evaluation_evidence evidence
        JOIN main.agent_control_verification_evaluation_receipts receipt
          ON receipt.evaluation_id = evidence.evaluation_id
         AND receipt.evidence_id = evidence.evidence_id
        WHERE evidence.evaluation_id IS NEW.evaluation_id
          AND evidence.evidence_id IS NEW.evidence_id
          AND evidence.receipt_id IS NEW.receipt_id
          AND evidence.marker_id IS NEW.marker_id
          AND evidence.evaluation_fingerprint IS NEW.evaluation_fingerprint
          AND evidence.provider_delivery_id IS NEW.provider_delivery_id
          AND evidence.evaluated_at IS NEW.committed_at
          AND receipt.receipt_id IS NEW.receipt_id
          AND receipt.marker_id IS NEW.marker_id
          AND receipt.evaluation_fingerprint IS NEW.evaluation_fingerprint
          AND receipt.provider_delivery_id IS NEW.provider_delivery_id
          AND receipt.accepted_at IS NEW.committed_at
      )
      BEGIN SELECT RAISE(ABORT, 'verification evaluation marker is inconsistent'); END
    `).unprepared;
    for (const table of [
      "agent_control_verification_evaluation_evidence",
      "agent_control_verification_evaluation_receipts",
      "agent_control_verification_evaluation_markers",
    ]) {
      yield* sql.unsafe(`
        CREATE TRIGGER main.${table}_no_update BEFORE UPDATE ON ${table}
        BEGIN SELECT RAISE(ABORT, 'verification evaluation evidence is immutable'); END
      `).unprepared;
      yield* sql.unsafe(`
        CREATE TRIGGER main.${table}_no_delete BEFORE DELETE ON ${table}
        BEGIN SELECT RAISE(ABORT, 'verification evaluation evidence is immutable'); END
      `).unprepared;
    }
    yield* injectFault("after-install");

    const mainSchema = yield* sql<{
      readonly type: string;
      readonly name: string;
      readonly tableName: string;
      readonly sql: string | null;
    }>`
      SELECT type, name, tbl_name AS "tableName", sql
      FROM main.sqlite_schema
      WHERE name IS NOT NULL
    `;
    const mainSchemaByName = new Map<string, ReadonlyArray<(typeof mainSchema)[number]>>();
    for (const row of mainSchema) {
      const canonicalName = row.name.toLowerCase();
      mainSchemaByName.set(canonicalName, [...(mainSchemaByName.get(canonicalName) ?? []), row]);
    }
    const exactMainSchemaRow = (name: string) => {
      const rows = mainSchemaByName.get(name.toLowerCase()) ?? [];
      return rows.length === 1 ? rows[0] : undefined;
    };
    for (const tableName of [
      "agent_control_verification_evaluation_evidence",
      "agent_control_verification_evaluation_receipts",
      "agent_control_verification_evaluation_markers",
    ] as const) {
      const row = exactMainSchemaRow(tableName);
      if (
        row?.type !== "table" ||
        row.tableName !== tableName ||
        row.sql === null ||
        !normalizeSchemaSql(row.sql).startsWith(`CREATE TABLE ${tableName} (`)
      ) {
        return yield* Effect.die(new Error(`migration 060 MAIN table audit failed: ${tableName}`));
      }
    }
    for (const [name, tableName, requiredSql] of migration060TriggerAudit) {
      const row = exactMainSchemaRow(name);
      const normalizedSql =
        row?.sql === null || row?.sql === undefined ? "" : normalizeSchemaSql(row.sql);
      if (
        row?.type !== "trigger" ||
        row.tableName !== tableName ||
        !normalizedSql.startsWith(`CREATE TRIGGER ${name} `) ||
        !normalizedSql.includes(` ON ${tableName} `) ||
        !normalizedSql.includes(requiredSql)
      ) {
        return yield* Effect.die(new Error(`migration 060 MAIN trigger audit failed: ${name}`));
      }
    }
    const handoffValidation = exactMainSchemaRow(
      "agent_control_verification_handoff_intent_validate",
    );
    if (
      handoffValidation?.type !== "trigger" ||
      handoffValidation.tableName !== "agent_control_verification_handoff_intents" ||
      handoffValidation.sql === null ||
      !normalizeSchemaSql(handoffValidation.sql).includes("prompt_template_version IS NULL") ||
      !normalizeSchemaSql(handoffValidation.sql).includes(
        AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT,
      )
    ) {
      return yield* Effect.die(new Error("migration 060 MAIN handoff trigger audit failed"));
    }
    for (const [name, tableName, unique, partial] of [
      [
        "idx_agent_control_verification_evaluation_provider_turn",
        "agent_control_verification_evaluation_evidence",
        1,
        0,
      ],
      [
        "idx_agent_control_verification_evaluation_candidate",
        "agent_control_verification_handoff_intents",
        0,
        1,
      ],
      [VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX, "orchestration_events", 1, 1],
    ] as const) {
      const row = exactMainSchemaRow(name);
      const normalizedSql =
        row?.sql === null || row?.sql === undefined ? "" : normalizeSchemaSql(row.sql);
      const indexFlags = yield* sql<{ readonly isUnique: number; readonly partial: number }>`
        SELECT "unique" AS "isUnique", partial
        FROM pragma_index_list(${tableName}, 'main')
        WHERE name = ${name}
      `;
      if (
        row?.type !== "index" ||
        row.tableName !== tableName ||
        !normalizedSql.startsWith(`CREATE ${unique === 1 ? "UNIQUE " : ""}INDEX ${name} `) ||
        (!normalizedSql.includes(` ON ${tableName}(`) &&
          !normalizedSql.includes(` ON ${tableName} (`)) ||
        indexFlags.length !== 1 ||
        indexFlags[0]?.isUnique !== unique ||
        indexFlags[0]?.partial !== partial
      ) {
        return yield* Effect.die(new Error(`migration 060 MAIN index audit failed: ${name}`));
      }
    }

    const violations = yield* sql<Record<string, unknown>>`PRAGMA main.foreign_key_check`;
    if (violations.length !== 0) {
      return yield* Effect.die(new Error("migration 060 introduced foreign-key violations"));
    }
    const integrity = yield* sql<{ readonly integrity_check: string }>`PRAGMA main.integrity_check`;
    if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") {
      return yield* Effect.die(new Error("migration 060 failed SQLite integrity validation"));
    }
  });

export default makeMigration060();
