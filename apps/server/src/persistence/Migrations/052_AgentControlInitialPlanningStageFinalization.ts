import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  parseCanonicalJson,
  sha256Utf8,
} from "../../agentControl/initialPlanning/eventEvidence.ts";
import {
  deriveInitialPlanningFinalizationCommandId,
  deriveInitialPlanningFinalizationMarkerId,
  deriveInitialPlanningLeaseReleaseEventId,
  deriveInitialPlanningResultEvidenceId,
  deriveInitialPlanningStageStartCommandId,
  deriveInitialPlanningStageStartedEventId,
  deriveInitialPlanningTerminalStageEventId,
  fingerprintInitialPlanningFinalization,
} from "../../agentControl/initialPlanning/finalizationIdentity.ts";

const quoteSqliteIdentifier = (identifier: string): string =>
  `"${identifier.replaceAll('"', '""')}"`;
const text = (column: string) =>
  `typeof(${column}) = 'text' AND length(${column}) > 0 AND trim(${column}) = ${column}`;
const positiveInteger = (column: string) => `typeof(${column}) = 'integer' AND ${column} >= 1`;
const sha256 = (column: string) =>
  `${text(column)} AND length(${column}) = 64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;
const timestamp = (column: string) => `
  ${text(column)}
  AND length(${column}) = 24
  AND ${column} GLOB
    '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
  AND strftime('%Y-%m-%dT%H:%M:%fZ', ${column}) = ${column}
`;
const nullableText = (column: string) => `(typeof(${column}) = 'null' OR (${text(column)}))`;
const nullablePositiveInteger = (column: string) =>
  `(typeof(${column}) = 'null' OR (${positiveInteger(column)}))`;
const nullableSha256 = (column: string) => `(typeof(${column}) = 'null' OR (${sha256(column)}))`;
const every = (predicates: ReadonlyArray<string>) => predicates.join(" AND ");

const stageIdentityKeys = [
  "projectId",
  "taskId",
  "stageRunId",
  "attemptId",
  "roleId",
  "stageKind",
  "stageOrdinal",
  "attemptOrdinal",
  "taskRevision",
  "githubIntakeSequence",
  "sourceIdentityFingerprint",
  "handoffId",
  "handoffFingerprint",
  "controlledThreadReservationId",
  "threadId",
  "providerDeliveryId",
  "providerInstanceId",
  "providerTurnId",
  "runtimeMode",
  "modelSelectionFingerprint",
  "leaseId",
  "leaseHolderId",
  "fenceToken",
] as const;
const leaseIdentityKeys = [
  "leaseId",
  "projectId",
  "taskId",
  "stageRunId",
  "attemptId",
  "taskRevision",
  "githubIntakeSequence",
  "sourceIdentityFingerprint",
  "holderId",
  "fenceToken",
  "handoffId",
  "handoffFingerprint",
  "controlledThreadReservationId",
  "threadId",
  "providerDeliveryId",
  "providerInstanceId",
  "providerTurnId",
  "runtimeMode",
  "modelSelectionFingerprint",
  "resultEvidenceId",
  "stageStatus",
  "releasedAt",
] as const;
const textPayloadKeys = new Set([
  "projectId",
  "taskId",
  "stageRunId",
  "attemptId",
  "roleId",
  "stageKind",
  "sourceIdentityFingerprint",
  "handoffId",
  "handoffFingerprint",
  "controlledThreadReservationId",
  "threadId",
  "providerDeliveryId",
  "providerInstanceId",
  "providerTurnId",
  "runtimeMode",
  "modelSelectionFingerprint",
  "leaseId",
  "leaseHolderId",
  "holderId",
  "resultEvidenceId",
  "status",
  "stageStatus",
  "startedAt",
  "finalizedAt",
  "releasedAt",
]);
const integerPayloadKeys = new Set([
  "stageOrdinal",
  "attemptOrdinal",
  "taskRevision",
  "githubIntakeSequence",
  "fenceToken",
]);
const jsonObject = (row: string, keys: ReadonlyArray<string>) =>
  `json_object(${keys.map((key) => `'${key}', json_extract(${row}.payload_json, '$.${key}')`).join(", ")})`;
const payloadTypes = (row: string, keys: ReadonlyArray<string>) =>
  keys
    .map((key) => {
      if (textPayloadKeys.has(key)) {
        return `json_type(${row}.payload_json, '$.${key}') = 'text'
          AND length(json_extract(${row}.payload_json, '$.${key}')) > 0
          AND trim(json_extract(${row}.payload_json, '$.${key}')) =
            json_extract(${row}.payload_json, '$.${key}')`;
      }
      if (integerPayloadKeys.has(key)) {
        return `json_type(${row}.payload_json, '$.${key}') = 'integer'
          AND json_extract(${row}.payload_json, '$.${key}') >= 1`;
      }
      return "1";
    })
    .join(" AND ");
const jsonIdentityEqual = (left: string, right: string, keys: ReadonlyArray<string>) =>
  keys
    .map(
      (key) =>
        `json_extract(${left}.payload_json, '$.${key}') IS json_extract(${right}.payload_json, '$.${key}')`,
    )
    .join(" AND ");
const eventStorage = (row: string) => `
  typeof(${row}.event_id) = 'text' AND length(${row}.event_id) > 0
  AND trim(${row}.event_id) = ${row}.event_id
  AND typeof(${row}.aggregate_kind) = 'text'
  AND typeof(${row}.stream_id) = 'text' AND length(${row}.stream_id) > 0
  AND trim(${row}.stream_id) = ${row}.stream_id
  AND typeof(${row}.stream_version) = 'integer' AND ${row}.stream_version >= 1
  AND typeof(${row}.event_type) = 'text'
  AND ${timestamp(`${row}.occurred_at`)}
  AND typeof(${row}.command_id) = 'text' AND length(${row}.command_id) > 0
  AND trim(${row}.command_id) = ${row}.command_id
  AND (${row}.causation_event_id IS NULL OR (
    typeof(${row}.causation_event_id) = 'text' AND length(${row}.causation_event_id) > 0
    AND trim(${row}.causation_event_id) = ${row}.causation_event_id
  ))
  AND typeof(${row}.correlation_id) = 'text' AND length(${row}.correlation_id) > 0
  AND trim(${row}.correlation_id) = ${row}.correlation_id
  AND typeof(${row}.actor_authority) = 'text'
  AND typeof(${row}.payload_json) = 'text'
  AND typeof(${row}.metadata_json) = 'text'
`;
const metadataPredicate = (row: string) => `
  json_valid(${row}.metadata_json) = 1
  AND json_type(${row}.metadata_json) = 'object'
  AND ${row}.metadata_json = '{"schemaVersion":1}'
`;
const stageEventPredicate = (row: string) => {
  const startedKeys = [...stageIdentityKeys, "status", "startedAt"];
  const finalizedKeys = [...stageIdentityKeys, "resultEvidenceId", "finalizedAt", "status"];
  return `
    ${eventStorage(row)}
    AND ${row}.aggregate_kind = 'stage-run'
    AND ${row}.actor_authority = 'system'
    AND ${metadataPredicate(row)}
    AND json_valid(${row}.payload_json) = 1
    AND json_type(${row}.payload_json) = 'object'
    AND ${row}.stream_id = json_extract(${row}.payload_json, '$.stageRunId')
    AND ${row}.command_id = ${row}.correlation_id
    AND (
      (
        ${row}.event_type = 'agentControl.stageRun.planningStarted'
        AND ${row}.stream_version = 2
        AND ${payloadTypes(row, startedKeys)}
        AND json_extract(${row}.payload_json, '$.roleId') = 'planning'
        AND json_extract(${row}.payload_json, '$.stageKind') = 'planning'
        AND json_extract(${row}.payload_json, '$.stageOrdinal') = 1
        AND json_extract(${row}.payload_json, '$.attemptOrdinal') = 1
        AND json_extract(${row}.payload_json, '$.status') = 'running'
        AND ${row}.occurred_at = json_extract(${row}.payload_json, '$.startedAt')
        AND strftime('%Y-%m-%dT%H:%M:%fZ',
          json_extract(${row}.payload_json, '$.startedAt')) =
          json_extract(${row}.payload_json, '$.startedAt')
        AND ${row}.payload_json = ${jsonObject(row, startedKeys)}
        AND EXISTS (
          SELECT 1 FROM agent_control_events prepared
          WHERE prepared.aggregate_kind = 'stage-run'
            AND prepared.stream_id = ${row}.stream_id
            AND prepared.stream_version = 1
            AND prepared.event_type = 'agentControl.stageRun.prepared'
            AND ${jsonIdentityEqual(row, "prepared", [
              "projectId",
              "taskId",
              "stageRunId",
              "attemptId",
              "roleId",
              "stageKind",
              "stageOrdinal",
              "attemptOrdinal",
              "taskRevision",
              "githubIntakeSequence",
              "sourceIdentityFingerprint",
            ])}
        )
      ) OR (
        ${row}.event_type IN (
          'agentControl.stageRun.planningSucceeded',
          'agentControl.stageRun.planningFailed',
          'agentControl.stageRun.planningCancelled'
        )
        AND ${row}.stream_version = 3
        AND ${payloadTypes(row, finalizedKeys)}
        AND json_extract(${row}.payload_json, '$.roleId') = 'planning'
        AND json_extract(${row}.payload_json, '$.stageKind') = 'planning'
        AND json_extract(${row}.payload_json, '$.stageOrdinal') = 1
        AND json_extract(${row}.payload_json, '$.attemptOrdinal') = 1
        AND json_extract(${row}.payload_json, '$.status') = CASE ${row}.event_type
          WHEN 'agentControl.stageRun.planningSucceeded' THEN 'succeeded'
          WHEN 'agentControl.stageRun.planningFailed' THEN 'failed'
          ELSE 'cancelled' END
        AND ${row}.occurred_at = json_extract(${row}.payload_json, '$.finalizedAt')
        AND strftime('%Y-%m-%dT%H:%M:%fZ',
          json_extract(${row}.payload_json, '$.finalizedAt')) =
          json_extract(${row}.payload_json, '$.finalizedAt')
        AND ${row}.payload_json = ${jsonObject(row, finalizedKeys)}
        AND EXISTS (
          SELECT 1 FROM agent_control_events started
          WHERE started.aggregate_kind = 'stage-run'
            AND started.stream_id = ${row}.stream_id
            AND started.stream_version = 2
            AND started.event_type = 'agentControl.stageRun.planningStarted'
            AND ${jsonIdentityEqual(row, "started", stageIdentityKeys)}
        )
      )
    )
  `;
};
const leaseEventPredicate = (row: string) => `
  ${eventStorage(row)}
  AND ${row}.aggregate_kind = 'stage-run-lease'
  AND ${row}.event_type = 'agentControl.stageRunLease.releasedAfterPlanning'
  AND ${row}.actor_authority = 'system'
  AND ${row}.stream_version >= 2
  AND ${metadataPredicate(row)}
  AND json_valid(${row}.payload_json) = 1
  AND json_type(${row}.payload_json) = 'object'
  AND ${payloadTypes(row, leaseIdentityKeys)}
  AND json_extract(${row}.payload_json, '$.stageStatus') IN (
    'succeeded', 'failed', 'cancelled'
  )
  AND ${row}.stream_id = json_extract(${row}.payload_json, '$.leaseId')
  AND ${row}.occurred_at = json_extract(${row}.payload_json, '$.releasedAt')
  AND strftime('%Y-%m-%dT%H:%M:%fZ',
    json_extract(${row}.payload_json, '$.releasedAt')) =
    json_extract(${row}.payload_json, '$.releasedAt')
  AND ${row}.command_id = ${row}.correlation_id
  AND ${row}.payload_json = ${jsonObject(row, leaseIdentityKeys)}
  AND (
    SELECT count(*) FROM agent_control_events prior
    WHERE prior.aggregate_kind = 'stage-run-lease'
      AND prior.stream_id = ${row}.stream_id
      AND prior.stream_version < ${row}.stream_version
  ) = ${row}.stream_version - 1
  AND EXISTS (
    SELECT 1 FROM agent_control_events reserved
    WHERE reserved.aggregate_kind = 'stage-run-lease'
      AND reserved.stream_id = ${row}.stream_id
      AND reserved.stream_version = 1
      AND reserved.event_type = 'agentControl.stageRunLease.reserved'
      AND json_extract(reserved.payload_json, '$.leaseId') =
        json_extract(${row}.payload_json, '$.leaseId')
      AND json_extract(reserved.payload_json, '$.projectId') =
        json_extract(${row}.payload_json, '$.projectId')
      AND json_extract(reserved.payload_json, '$.taskId') =
        json_extract(${row}.payload_json, '$.taskId')
      AND json_extract(reserved.payload_json, '$.stageRunId') =
        json_extract(${row}.payload_json, '$.stageRunId')
      AND json_extract(reserved.payload_json, '$.attemptId') =
        json_extract(${row}.payload_json, '$.attemptId')
      AND json_extract(reserved.payload_json, '$.taskRevision') =
        json_extract(${row}.payload_json, '$.taskRevision')
      AND json_extract(reserved.payload_json, '$.githubIntakeSequence') =
        json_extract(${row}.payload_json, '$.githubIntakeSequence')
      AND json_extract(reserved.payload_json, '$.sourceIdentityFingerprint') =
        json_extract(${row}.payload_json, '$.sourceIdentityFingerprint')
      AND json_extract(reserved.payload_json, '$.holderId') =
        json_extract(${row}.payload_json, '$.holderId')
      AND json_extract(reserved.payload_json, '$.fenceToken') =
        json_extract(${row}.payload_json, '$.fenceToken')
  )
  AND NOT EXISTS (
    SELECT 1 FROM agent_control_events prior
    WHERE prior.aggregate_kind = 'stage-run-lease'
      AND prior.stream_id = ${row}.stream_id
      AND prior.stream_version > 1
      AND prior.stream_version < ${row}.stream_version
      AND (
        prior.event_type <> 'agentControl.stageRunLease.renewed'
        OR json_extract(prior.payload_json, '$.leaseId') IS NOT
          json_extract(${row}.payload_json, '$.leaseId')
        OR json_extract(prior.payload_json, '$.stageRunId') IS NOT
          json_extract(${row}.payload_json, '$.stageRunId')
        OR json_extract(prior.payload_json, '$.attemptId') IS NOT
          json_extract(${row}.payload_json, '$.attemptId')
        OR json_extract(prior.payload_json, '$.holderId') IS NOT
          json_extract(${row}.payload_json, '$.holderId')
        OR json_extract(prior.payload_json, '$.fenceToken') IS NOT
          json_extract(${row}.payload_json, '$.fenceToken')
      )
  )
`;

const stageStartedStoragePredicate = (row: string) =>
  every([
    text(`${row}.start_command_id`),
    sha256(`${row}.start_fingerprint`),
    text(`${row}.handoff_id`),
    sha256(`${row}.handoff_fingerprint`),
    text(`${row}.project_id`),
    text(`${row}.task_id`),
    positiveInteger(`${row}.task_revision`),
    positiveInteger(`${row}.github_intake_sequence`),
    sha256(`${row}.source_identity_fingerprint`),
    text(`${row}.controlled_thread_reservation_id`),
    text(`${row}.thread_id`),
    text(`${row}.stage_run_id`),
    text(`${row}.attempt_id`),
    text(`${row}.lease_id`),
    text(`${row}.lease_holder_id`),
    positiveInteger(`${row}.fence_token`),
    text(`${row}.provider_delivery_id`),
    text(`${row}.provider_instance_id`),
    text(`${row}.provider_turn_id`),
    `typeof(${row}.runtime_mode) = 'text' AND ${row}.runtime_mode IN (
      'approval-required', 'full-access'
    )`,
    sha256(`${row}.model_selection_fingerprint`),
    timestamp(`${row}.provider_accepted_at`),
    positiveInteger(`${row}.delivery_revision`),
    text(`${row}.orchestration_started_event_id`),
    positiveInteger(`${row}.orchestration_started_sequence`),
    `typeof(${row}.orchestration_started_stream_version) = 'integer'
      AND ${row}.orchestration_started_stream_version >= 0`,
    text(`${row}.stage_event_id`),
    positiveInteger(`${row}.stage_event_sequence`),
    `typeof(${row}.stage_event_stream_version) = 'integer'
      AND ${row}.stage_event_stream_version = 2`,
    timestamp(`${row}.recorded_at`),
  ]);

const stageStartedProjectionPredicate = (row: string, existing: boolean) =>
  existing
    ? `(
      (
        stage_state.status = 'running'
        AND stage_state.revision = 2
        AND stage_state.last_event_sequence = ${row}.stage_event_sequence
        AND lease_state.status = 'reserved'
      ) OR EXISTS (
        SELECT 1
        FROM agent_control_initial_planning_result_evidence terminal
        WHERE terminal.handoff_id = ${row}.handoff_id
          AND terminal.stage_run_id = ${row}.stage_run_id
          AND terminal.attempt_id = ${row}.attempt_id
          AND terminal.lease_id = ${row}.lease_id
          AND terminal.stage_event_stream_version = stage_state.revision
          AND terminal.stage_event_sequence = stage_state.last_event_sequence
          AND stage_state.status = terminal.outcome
          AND lease_state.status = 'released'
          AND terminal.lease_event_stream_version = lease_state.revision
          AND terminal.lease_event_sequence = lease_state.last_event_sequence
          AND terminal.finalized_at = lease_state.released_at
      )
    )`
    : `stage_state.status = 'running'
      AND stage_state.revision = 2
      AND stage_state.last_event_sequence = ${row}.stage_event_sequence
      AND lease_state.status = 'reserved'`;

const recoverableLegacyDeliveryPredicate = (row: string) => `
  NOT EXISTS (
    SELECT 1 FROM agent_control_initial_planning_result_evidence result
    WHERE result.handoff_id = ${row}.handoff_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM agent_control_initial_planning_finalization_receipts receipt
    WHERE receipt.handoff_id = ${row}.handoff_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM agent_control_initial_planning_finalization_markers marker
    WHERE marker.handoff_id = ${row}.handoff_id
  )
  AND (
    (
      delivery.state = 'provider-started'
      AND delivery.revision = ${row}.delivery_revision
      AND delivery.interrupt_requested = 0
      AND delivery.terminal_at IS NULL
      AND delivery.last_error_code IS NULL
      AND delivery.updated_at = ${row}.provider_accepted_at
    ) OR (
      delivery.state = 'interrupt-requested'
      AND delivery.revision = ${row}.delivery_revision + 1
      AND delivery.interrupt_requested = 1
      AND delivery.terminal_at IS NULL
      AND delivery.last_error_code IS NULL
      AND delivery.updated_at >= ${row}.provider_accepted_at
    ) OR (
      delivery.state = 'ambiguous'
      AND delivery.revision = ${row}.delivery_revision + 1 + delivery.interrupt_requested
      AND delivery.terminal_at IS NOT NULL
      AND delivery.last_error_code = 'provider-acceptance-ambiguous'
      AND delivery.updated_at = delivery.terminal_at
      AND delivery.terminal_at >= ${row}.provider_accepted_at
    ) OR (
      delivery.state IN ('completed', 'failed', 'interrupted')
      AND (
        (delivery.interrupt_requested = 0 AND delivery.revision IN (
          ${row}.delivery_revision + 1, ${row}.delivery_revision + 2
        ))
        OR
        (delivery.interrupt_requested = 1 AND delivery.revision IN (
          ${row}.delivery_revision + 2, ${row}.delivery_revision + 3
        ))
      )
      AND delivery.terminal_at IS NOT NULL
      AND delivery.updated_at = delivery.terminal_at
      AND delivery.terminal_at >= ${row}.provider_accepted_at
      AND (
        (delivery.state = 'completed' AND delivery.last_error_code IS NULL)
        OR
        (delivery.state = 'failed'
          AND delivery.last_error_code IN ('provider-aborted', 'provider-defect'))
        OR
        (delivery.state = 'interrupted'
          AND delivery.last_error_code = 'provider-aborted')
      )
      AND EXISTS (
        SELECT 1
        FROM orchestration_events terminal
        WHERE terminal.aggregate_kind = 'thread'
          AND terminal.stream_id = ${row}.thread_id
          AND terminal.event_type = 'thread.session-set'
          AND terminal.actor_kind = 'provider'
          AND terminal.occurred_at = delivery.terminal_at
          AND json_extract(terminal.payload_json, '$.threadId') = ${row}.thread_id
          AND json_extract(terminal.payload_json, '$.session.threadId') = ${row}.thread_id
          AND json_type(terminal.payload_json, '$.session.activeTurnId') = 'null'
          AND json_extract(terminal.payload_json, '$.session.providerInstanceId') =
            ${row}.provider_instance_id
          AND json_extract(terminal.payload_json, '$.session.runtimeMode') = ${row}.runtime_mode
          AND json_extract(terminal.payload_json, '$.session.status') =
            CASE delivery.state WHEN 'failed' THEN 'error' ELSE 'ready' END
      )
    )
  )
`;

const finalizedLegacyDeliveryPredicate = (row: string) => `
  EXISTS (
    SELECT 1
    FROM agent_control_initial_planning_result_evidence delivered_result
    WHERE delivered_result.handoff_id = ${row}.handoff_id
      AND delivered_result.delivery_revision = delivery.revision
      AND delivered_result.delivery_terminal_state = delivery.state
      AND delivered_result.terminal_at = delivery.terminal_at
  )
`;

const stageStartedPredicate = (row: string, existing = false) => `
  ${stageStartedStoragePredicate(row)}
  AND EXISTS (
    SELECT 1
    FROM agent_control_events event
    JOIN agent_control_initial_planning_handoff_intents intent
      ON intent.handoff_id = ${row}.handoff_id
    JOIN agent_control_initial_planning_handoff_accepted accepted
      ON accepted.handoff_id = intent.handoff_id
    JOIN agent_control_initial_planning_deliveries delivery
      ON delivery.handoff_id = intent.handoff_id
    JOIN agent_control_initial_planning_delivery_attestations attestation
      ON attestation.provider_delivery_id = delivery.provider_delivery_id
    JOIN orchestration_events orchestration
      ON orchestration.event_id = ${row}.orchestration_started_event_id
    JOIN agent_control_stage_run_states stage_state
      ON stage_state.stage_run_id = ${row}.stage_run_id
    JOIN agent_control_stage_run_lease_states lease_state
      ON lease_state.lease_id = ${row}.lease_id
    WHERE event.event_id = ${row}.stage_event_id
      AND event.sequence = ${row}.stage_event_sequence
      AND event.stream_id = ${row}.stage_run_id
      AND event.stream_version = ${row}.stage_event_stream_version
      AND event.event_type = 'agentControl.stageRun.planningStarted'
      AND event.command_id = ${row}.start_command_id
      AND json_extract(event.payload_json, '$.handoffId') = ${row}.handoff_id
      AND json_extract(event.payload_json, '$.handoffFingerprint') = ${row}.handoff_fingerprint
      AND json_extract(event.payload_json, '$.projectId') = ${row}.project_id
      AND json_extract(event.payload_json, '$.taskId') = ${row}.task_id
      AND json_extract(event.payload_json, '$.taskRevision') = ${row}.task_revision
      AND json_extract(event.payload_json, '$.githubIntakeSequence') =
        ${row}.github_intake_sequence
      AND json_extract(event.payload_json, '$.sourceIdentityFingerprint') =
        ${row}.source_identity_fingerprint
      AND json_extract(event.payload_json, '$.controlledThreadReservationId') =
        ${row}.controlled_thread_reservation_id
      AND json_extract(event.payload_json, '$.threadId') = ${row}.thread_id
      AND json_extract(event.payload_json, '$.stageRunId') = ${row}.stage_run_id
      AND json_extract(event.payload_json, '$.attemptId') = ${row}.attempt_id
      AND json_extract(event.payload_json, '$.leaseId') = ${row}.lease_id
      AND json_extract(event.payload_json, '$.leaseHolderId') = ${row}.lease_holder_id
      AND json_extract(event.payload_json, '$.fenceToken') = ${row}.fence_token
      AND json_extract(event.payload_json, '$.providerDeliveryId') =
        ${row}.provider_delivery_id
      AND json_extract(event.payload_json, '$.providerInstanceId') =
        ${row}.provider_instance_id
      AND json_extract(event.payload_json, '$.providerTurnId') = ${row}.provider_turn_id
      AND json_extract(event.payload_json, '$.runtimeMode') = ${row}.runtime_mode
      AND json_extract(event.payload_json, '$.modelSelectionFingerprint') =
        ${row}.model_selection_fingerprint
      AND json_extract(event.payload_json, '$.startedAt') = ${row}.provider_accepted_at
      AND intent.handoff_fingerprint = ${row}.handoff_fingerprint
      AND intent.project_id = ${row}.project_id
      AND intent.task_id = ${row}.task_id
      AND intent.task_revision = ${row}.task_revision
      AND intent.github_intake_sequence = ${row}.github_intake_sequence
      AND intent.source_identity_fingerprint = ${row}.source_identity_fingerprint
      AND intent.controlled_thread_reservation_id = ${row}.controlled_thread_reservation_id
      AND intent.thread_id = ${row}.thread_id
      AND intent.stage_run_id = ${row}.stage_run_id
      AND intent.attempt_id = ${row}.attempt_id
      AND intent.lease_id = ${row}.lease_id
      AND intent.lease_holder_id = ${row}.lease_holder_id
      AND intent.fence_token = ${row}.fence_token
      AND accepted.handoff_fingerprint = ${row}.handoff_fingerprint
      AND accepted.controlled_thread_reservation_id = ${row}.controlled_thread_reservation_id
      AND accepted.thread_id = ${row}.thread_id
      AND accepted.provider_delivery_id = ${row}.provider_delivery_id
      AND delivery.provider_delivery_id = ${row}.provider_delivery_id
      AND delivery.handoff_fingerprint = ${row}.handoff_fingerprint
      AND delivery.controlled_thread_reservation_id = ${row}.controlled_thread_reservation_id
      AND delivery.thread_id = ${row}.thread_id
      AND delivery.provider_instance_id = ${row}.provider_instance_id
      AND delivery.provider_turn_id = ${row}.provider_turn_id
      AND delivery.provider_accepted_at = ${row}.provider_accepted_at
      AND ${
        existing
          ? `(
            (${recoverableLegacyDeliveryPredicate(row)})
            OR (${finalizedLegacyDeliveryPredicate(row)})
          )`
          : `delivery.revision = ${row}.delivery_revision`
      }
      AND intent.runtime_mode = ${row}.runtime_mode
      AND attestation.provider_instance_id = ${row}.provider_instance_id
      AND attestation.model_selection_fingerprint = ${row}.model_selection_fingerprint
      AND orchestration.sequence = ${row}.orchestration_started_sequence
      AND orchestration.stream_version = ${row}.orchestration_started_stream_version
      AND orchestration.stream_id = ${row}.thread_id
      AND orchestration.event_type = 'thread.session-set'
      AND orchestration.actor_kind = 'provider'
      AND orchestration.occurred_at = ${row}.provider_accepted_at
      AND json_extract(orchestration.payload_json, '$.threadId') = ${row}.thread_id
      AND json_extract(orchestration.payload_json, '$.session.threadId') = ${row}.thread_id
      AND json_extract(orchestration.payload_json, '$.session.activeTurnId') =
        ${row}.provider_turn_id
      AND json_extract(orchestration.payload_json, '$.session.providerInstanceId') =
        ${row}.provider_instance_id
      AND json_extract(orchestration.payload_json, '$.session.runtimeMode') = ${row}.runtime_mode
      AND json_extract(orchestration.payload_json, '$.session.status') = 'running'
      AND lease_state.project_id = ${row}.project_id
      AND lease_state.task_id = ${row}.task_id
      AND lease_state.stage_run_id = ${row}.stage_run_id
      AND lease_state.attempt_id = ${row}.attempt_id
      AND lease_state.holder_id = ${row}.lease_holder_id
      AND lease_state.fence_token = ${row}.fence_token
      AND ${row}.recorded_at = ${row}.provider_accepted_at
      AND ${stageStartedProjectionPredicate(row, existing)}
  )
`;

const resultEvidenceStoragePredicate = (row: string) =>
  every([
    text(`${row}.result_evidence_id`),
    text(`${row}.finalization_command_id`),
    sha256(`${row}.finalization_fingerprint`),
    `typeof(${row}.outcome) = 'text'
      AND ${row}.outcome IN ('succeeded', 'failed', 'cancelled')`,
    text(`${row}.handoff_id`),
    sha256(`${row}.handoff_fingerprint`),
    text(`${row}.project_id`),
    text(`${row}.task_id`),
    positiveInteger(`${row}.task_revision`),
    positiveInteger(`${row}.github_intake_sequence`),
    sha256(`${row}.source_identity_fingerprint`),
    text(`${row}.controlled_thread_reservation_id`),
    text(`${row}.thread_id`),
    text(`${row}.stage_run_id`),
    text(`${row}.attempt_id`),
    text(`${row}.lease_id`),
    text(`${row}.lease_holder_id`),
    positiveInteger(`${row}.fence_token`),
    text(`${row}.provider_delivery_id`),
    text(`${row}.provider_instance_id`),
    text(`${row}.provider_turn_id`),
    `typeof(${row}.runtime_mode) = 'text'
      AND ${row}.runtime_mode IN ('approval-required', 'full-access')`,
    sha256(`${row}.model_selection_fingerprint`),
    `typeof(${row}.delivery_terminal_state) = 'text'
      AND ${row}.delivery_terminal_state IN ('completed', 'failed', 'interrupted')`,
    positiveInteger(`${row}.delivery_revision`),
    timestamp(`${row}.terminal_at`),
    text(`${row}.orchestration_started_event_id`),
    positiveInteger(`${row}.orchestration_started_sequence`),
    text(`${row}.orchestration_terminal_event_id`),
    positiveInteger(`${row}.orchestration_terminal_sequence`),
    nullableText(`${row}.plan_id`),
    nullableText(`${row}.plan_event_id`),
    nullablePositiveInteger(`${row}.plan_event_sequence`),
    nullableText(`${row}.proposed_plan_json`),
    nullableSha256(`${row}.proposed_plan_digest`),
    text(`${row}.stage_event_id`),
    positiveInteger(`${row}.stage_event_sequence`),
    `typeof(${row}.stage_event_stream_version) = 'integer'
      AND ${row}.stage_event_stream_version = 3`,
    text(`${row}.lease_event_id`),
    positiveInteger(`${row}.lease_event_sequence`),
    `typeof(${row}.lease_event_stream_version) = 'integer'
      AND ${row}.lease_event_stream_version >= 2`,
    timestamp(`${row}.finalized_at`),
  ]);

const canonicalPlanPredicate = (row: string, planEvent: string, projection: string) => `
  ${row}.outcome = 'succeeded'
  AND ${text(`${row}.plan_id`)}
  AND ${text(`${row}.plan_event_id`)}
  AND ${positiveInteger(`${row}.plan_event_sequence`)}
  AND ${text(`${row}.proposed_plan_json`)}
  AND json_valid(${row}.proposed_plan_json) = 1
  AND json_type(${row}.proposed_plan_json) = 'object'
  AND ${sha256(`${row}.proposed_plan_digest`)}
  AND ${planEvent}.event_type = 'thread.proposed-plan-upserted'
  AND ${planEvent}.actor_kind = 'provider'
  AND ${planEvent}.sequence = ${row}.plan_event_sequence
  AND ${planEvent}.stream_id = ${row}.thread_id
  AND json_extract(${planEvent}.payload_json, '$.threadId') = ${row}.thread_id
  AND json_extract(${planEvent}.payload_json, '$.proposedPlan.id') = ${row}.plan_id
  AND json_extract(${planEvent}.payload_json, '$.proposedPlan.turnId') = ${row}.provider_turn_id
  AND json_type(${planEvent}.payload_json, '$.proposedPlan.planMarkdown') = 'text'
  AND length(trim(json_extract(
    ${planEvent}.payload_json, '$.proposedPlan.planMarkdown'
  ))) > 0
  AND json_type(${planEvent}.payload_json, '$.proposedPlan.implementedAt') = 'null'
  AND json_type(
    ${planEvent}.payload_json, '$.proposedPlan.implementationThreadId'
  ) = 'null'
  AND ${row}.proposed_plan_json = json_object(
    'createdAt', json_extract(${planEvent}.payload_json, '$.proposedPlan.createdAt'),
    'id', json_extract(${planEvent}.payload_json, '$.proposedPlan.id'),
    'implementationThreadId',
      json_extract(${planEvent}.payload_json, '$.proposedPlan.implementationThreadId'),
    'implementedAt', json_extract(${planEvent}.payload_json, '$.proposedPlan.implementedAt'),
    'planMarkdown', json_extract(${planEvent}.payload_json, '$.proposedPlan.planMarkdown'),
    'turnId', json_extract(${planEvent}.payload_json, '$.proposedPlan.turnId'),
    'updatedAt', json_extract(${planEvent}.payload_json, '$.proposedPlan.updatedAt')
  )
  AND ${projection}.plan_id = ${row}.plan_id
  AND ${projection}.thread_id = ${row}.thread_id
  AND ${projection}.turn_id = ${row}.provider_turn_id
  AND ${projection}.plan_markdown =
    json_extract(${planEvent}.payload_json, '$.proposedPlan.planMarkdown')
  AND ${projection}.implemented_at IS NULL
  AND ${projection}.implementation_thread_id IS NULL
  AND ${projection}.created_at =
    json_extract(${planEvent}.payload_json, '$.proposedPlan.createdAt')
  AND ${projection}.updated_at =
    json_extract(${planEvent}.payload_json, '$.proposedPlan.updatedAt')
`;

const resultEvidencePredicate = (row: string) => `
  ${resultEvidenceStoragePredicate(row)}
  AND EXISTS (
    SELECT 1
    FROM agent_control_initial_planning_stage_started started
    JOIN agent_control_initial_planning_deliveries delivery
      ON delivery.handoff_id = started.handoff_id
    JOIN agent_control_events stage_event ON stage_event.event_id = ${row}.stage_event_id
    JOIN agent_control_events lease_event ON lease_event.event_id = ${row}.lease_event_id
    JOIN orchestration_events orchestration_started
      ON orchestration_started.event_id = ${row}.orchestration_started_event_id
    JOIN orchestration_events orchestration_terminal
      ON orchestration_terminal.event_id = ${row}.orchestration_terminal_event_id
    LEFT JOIN orchestration_events plan_event ON plan_event.event_id = ${row}.plan_event_id
    LEFT JOIN projection_thread_proposed_plans plan_projection
      ON plan_projection.plan_id = ${row}.plan_id
    JOIN agent_control_stage_run_states stage_state
      ON stage_state.stage_run_id = ${row}.stage_run_id
    JOIN agent_control_stage_run_lease_states lease_state
      ON lease_state.lease_id = ${row}.lease_id
    WHERE started.handoff_id = ${row}.handoff_id
      AND started.handoff_fingerprint = ${row}.handoff_fingerprint
      AND started.project_id = ${row}.project_id
      AND started.task_id = ${row}.task_id
      AND started.task_revision = ${row}.task_revision
      AND started.github_intake_sequence = ${row}.github_intake_sequence
      AND started.source_identity_fingerprint = ${row}.source_identity_fingerprint
      AND started.controlled_thread_reservation_id = ${row}.controlled_thread_reservation_id
      AND started.thread_id = ${row}.thread_id
      AND started.stage_run_id = ${row}.stage_run_id
      AND started.attempt_id = ${row}.attempt_id
      AND started.lease_id = ${row}.lease_id
      AND started.lease_holder_id = ${row}.lease_holder_id
      AND started.fence_token = ${row}.fence_token
      AND started.provider_delivery_id = ${row}.provider_delivery_id
      AND started.provider_instance_id = ${row}.provider_instance_id
      AND started.provider_turn_id = ${row}.provider_turn_id
      AND started.runtime_mode = ${row}.runtime_mode
      AND started.model_selection_fingerprint = ${row}.model_selection_fingerprint
      AND delivery.provider_delivery_id = ${row}.provider_delivery_id
      AND delivery.handoff_fingerprint = ${row}.handoff_fingerprint
      AND delivery.controlled_thread_reservation_id = ${row}.controlled_thread_reservation_id
      AND delivery.thread_id = ${row}.thread_id
      AND delivery.provider_instance_id = ${row}.provider_instance_id
      AND delivery.provider_turn_id = ${row}.provider_turn_id
      AND delivery.provider_accepted_at = started.provider_accepted_at
      AND delivery.state = ${row}.delivery_terminal_state
      AND delivery.revision = ${row}.delivery_revision
      AND delivery.terminal_at = ${row}.terminal_at
      AND orchestration_started.sequence = ${row}.orchestration_started_sequence
      AND orchestration_started.stream_id = ${row}.thread_id
      AND orchestration_started.event_type = 'thread.session-set'
      AND orchestration_started.actor_kind = 'provider'
      AND orchestration_started.occurred_at = started.provider_accepted_at
      AND json_extract(orchestration_started.payload_json, '$.session.activeTurnId') =
        ${row}.provider_turn_id
      AND json_extract(orchestration_started.payload_json, '$.session.providerInstanceId') =
        ${row}.provider_instance_id
      AND json_extract(orchestration_started.payload_json, '$.session.runtimeMode') =
        ${row}.runtime_mode
      AND orchestration_terminal.sequence = ${row}.orchestration_terminal_sequence
      AND orchestration_terminal.stream_id = ${row}.thread_id
      AND orchestration_terminal.event_type = 'thread.session-set'
      AND orchestration_terminal.actor_kind = 'provider'
      AND orchestration_terminal.occurred_at = ${row}.terminal_at
      AND json_type(orchestration_terminal.payload_json, '$.session.activeTurnId') = 'null'
      AND json_extract(orchestration_terminal.payload_json, '$.session.providerInstanceId') =
        ${row}.provider_instance_id
      AND json_extract(orchestration_terminal.payload_json, '$.session.runtimeMode') =
        ${row}.runtime_mode
      AND (
        (${canonicalPlanPredicate(row, "plan_event", "plan_projection")})
        OR (
          ${row}.outcome IN ('failed', 'cancelled')
          AND typeof(${row}.plan_id) = 'null'
          AND typeof(${row}.plan_event_id) = 'null'
          AND typeof(${row}.plan_event_sequence) = 'null'
          AND typeof(${row}.proposed_plan_json) = 'null'
          AND typeof(${row}.proposed_plan_digest) = 'null'
          AND plan_event.event_id IS NULL
          AND plan_projection.plan_id IS NULL
        )
      )
      AND stage_event.sequence = ${row}.stage_event_sequence
      AND stage_event.stream_id = ${row}.stage_run_id
      AND stage_event.stream_version = ${row}.stage_event_stream_version
      AND stage_event.command_id = ${row}.finalization_command_id
      AND stage_event.event_type = CASE ${row}.outcome
        WHEN 'succeeded' THEN 'agentControl.stageRun.planningSucceeded'
        WHEN 'failed' THEN 'agentControl.stageRun.planningFailed'
        ELSE 'agentControl.stageRun.planningCancelled' END
      AND json_extract(stage_event.payload_json, '$.resultEvidenceId') =
        ${row}.result_evidence_id
      AND json_extract(stage_event.payload_json, '$.status') = ${row}.outcome
      AND json_extract(stage_event.payload_json, '$.finalizedAt') = ${row}.finalized_at
      AND json_extract(stage_event.payload_json, '$.projectId') = ${row}.project_id
      AND json_extract(stage_event.payload_json, '$.taskId') = ${row}.task_id
      AND json_extract(stage_event.payload_json, '$.stageRunId') = ${row}.stage_run_id
      AND json_extract(stage_event.payload_json, '$.attemptId') = ${row}.attempt_id
      AND json_extract(stage_event.payload_json, '$.roleId') = 'planning'
      AND json_extract(stage_event.payload_json, '$.stageKind') = 'planning'
      AND json_extract(stage_event.payload_json, '$.stageOrdinal') = 1
      AND json_extract(stage_event.payload_json, '$.attemptOrdinal') = 1
      AND json_extract(stage_event.payload_json, '$.taskRevision') = ${row}.task_revision
      AND json_extract(stage_event.payload_json, '$.githubIntakeSequence') =
        ${row}.github_intake_sequence
      AND json_extract(stage_event.payload_json, '$.sourceIdentityFingerprint') =
        ${row}.source_identity_fingerprint
      AND json_extract(stage_event.payload_json, '$.handoffId') = ${row}.handoff_id
      AND json_extract(stage_event.payload_json, '$.handoffFingerprint') =
        ${row}.handoff_fingerprint
      AND json_extract(stage_event.payload_json, '$.controlledThreadReservationId') =
        ${row}.controlled_thread_reservation_id
      AND json_extract(stage_event.payload_json, '$.threadId') = ${row}.thread_id
      AND json_extract(stage_event.payload_json, '$.providerDeliveryId') =
        ${row}.provider_delivery_id
      AND json_extract(stage_event.payload_json, '$.providerInstanceId') =
        ${row}.provider_instance_id
      AND json_extract(stage_event.payload_json, '$.providerTurnId') = ${row}.provider_turn_id
      AND json_extract(stage_event.payload_json, '$.runtimeMode') = ${row}.runtime_mode
      AND json_extract(stage_event.payload_json, '$.modelSelectionFingerprint') =
        ${row}.model_selection_fingerprint
      AND json_extract(stage_event.payload_json, '$.leaseId') = ${row}.lease_id
      AND json_extract(stage_event.payload_json, '$.leaseHolderId') = ${row}.lease_holder_id
      AND json_extract(stage_event.payload_json, '$.fenceToken') = ${row}.fence_token
      AND lease_event.sequence = ${row}.lease_event_sequence
      AND lease_event.stream_id = ${row}.lease_id
      AND lease_event.stream_version = ${row}.lease_event_stream_version
      AND lease_event.command_id = ${row}.finalization_command_id
      AND lease_event.event_type = 'agentControl.stageRunLease.releasedAfterPlanning'
      AND json_extract(lease_event.payload_json, '$.resultEvidenceId') =
        ${row}.result_evidence_id
      AND json_extract(lease_event.payload_json, '$.stageStatus') = ${row}.outcome
      AND json_extract(lease_event.payload_json, '$.releasedAt') = ${row}.finalized_at
      AND json_extract(lease_event.payload_json, '$.leaseId') = ${row}.lease_id
      AND json_extract(lease_event.payload_json, '$.projectId') = ${row}.project_id
      AND json_extract(lease_event.payload_json, '$.taskId') = ${row}.task_id
      AND json_extract(lease_event.payload_json, '$.stageRunId') = ${row}.stage_run_id
      AND json_extract(lease_event.payload_json, '$.attemptId') = ${row}.attempt_id
      AND json_extract(lease_event.payload_json, '$.taskRevision') = ${row}.task_revision
      AND json_extract(lease_event.payload_json, '$.githubIntakeSequence') =
        ${row}.github_intake_sequence
      AND json_extract(lease_event.payload_json, '$.sourceIdentityFingerprint') =
        ${row}.source_identity_fingerprint
      AND json_extract(lease_event.payload_json, '$.holderId') = ${row}.lease_holder_id
      AND json_extract(lease_event.payload_json, '$.fenceToken') = ${row}.fence_token
      AND json_extract(lease_event.payload_json, '$.handoffId') = ${row}.handoff_id
      AND json_extract(lease_event.payload_json, '$.handoffFingerprint') =
        ${row}.handoff_fingerprint
      AND json_extract(lease_event.payload_json, '$.controlledThreadReservationId') =
        ${row}.controlled_thread_reservation_id
      AND json_extract(lease_event.payload_json, '$.threadId') = ${row}.thread_id
      AND json_extract(lease_event.payload_json, '$.providerDeliveryId') =
        ${row}.provider_delivery_id
      AND json_extract(lease_event.payload_json, '$.providerInstanceId') =
        ${row}.provider_instance_id
      AND json_extract(lease_event.payload_json, '$.providerTurnId') = ${row}.provider_turn_id
      AND json_extract(lease_event.payload_json, '$.runtimeMode') = ${row}.runtime_mode
      AND json_extract(lease_event.payload_json, '$.modelSelectionFingerprint') =
        ${row}.model_selection_fingerprint
      AND stage_state.status = ${row}.outcome
      AND stage_state.revision = ${row}.stage_event_stream_version
      AND stage_state.last_event_sequence = ${row}.stage_event_sequence
      AND lease_state.status = 'released'
      AND lease_state.project_id = ${row}.project_id
      AND lease_state.task_id = ${row}.task_id
      AND lease_state.stage_run_id = ${row}.stage_run_id
      AND lease_state.attempt_id = ${row}.attempt_id
      AND lease_state.holder_id = ${row}.lease_holder_id
      AND lease_state.fence_token = ${row}.fence_token
      AND lease_state.revision = ${row}.lease_event_stream_version
      AND lease_state.last_event_sequence = ${row}.lease_event_sequence
      AND lease_state.released_at = ${row}.finalized_at
      AND ${row}.terminal_at = ${row}.finalized_at
  )
`;

const receiptStoragePredicate = (row: string) =>
  every([
    text(`${row}.finalization_command_id`),
    sha256(`${row}.finalization_fingerprint`),
    text(`${row}.result_evidence_id`),
    text(`${row}.handoff_id`),
    `typeof(${row}.outcome) = 'text'
      AND ${row}.outcome IN ('succeeded', 'failed', 'cancelled')`,
    text(`${row}.stage_event_id`),
    positiveInteger(`${row}.stage_event_sequence`),
    text(`${row}.lease_event_id`),
    positiveInteger(`${row}.lease_event_sequence`),
    timestamp(`${row}.accepted_at`),
  ]);

const receiptPredicate = (row: string) => `
  ${receiptStoragePredicate(row)}
  AND EXISTS (
    SELECT 1 FROM agent_control_initial_planning_result_evidence evidence
    WHERE evidence.finalization_command_id = ${row}.finalization_command_id
      AND evidence.finalization_fingerprint = ${row}.finalization_fingerprint
      AND evidence.result_evidence_id = ${row}.result_evidence_id
      AND evidence.handoff_id = ${row}.handoff_id
      AND evidence.outcome = ${row}.outcome
      AND evidence.stage_event_id = ${row}.stage_event_id
      AND evidence.stage_event_sequence = ${row}.stage_event_sequence
      AND evidence.lease_event_id = ${row}.lease_event_id
      AND evidence.lease_event_sequence = ${row}.lease_event_sequence
      AND evidence.finalized_at = ${row}.accepted_at
  )
`;

const markerStoragePredicate = (row: string) =>
  every([
    text(`${row}.marker_id`),
    sha256(`${row}.marker_fingerprint`),
    text(`${row}.finalization_command_id`),
    text(`${row}.result_evidence_id`),
    text(`${row}.handoff_id`),
    timestamp(`${row}.committed_at`),
  ]);

const markerPredicate = (row: string) => `
  ${markerStoragePredicate(row)}
  AND EXISTS (
    SELECT 1
    FROM agent_control_initial_planning_finalization_receipts receipt
    JOIN agent_control_initial_planning_result_evidence evidence
      ON evidence.finalization_command_id = receipt.finalization_command_id
     AND evidence.finalization_fingerprint = receipt.finalization_fingerprint
     AND evidence.result_evidence_id = receipt.result_evidence_id
     AND evidence.handoff_id = receipt.handoff_id
     AND evidence.outcome = receipt.outcome
     AND evidence.stage_event_id = receipt.stage_event_id
     AND evidence.stage_event_sequence = receipt.stage_event_sequence
     AND evidence.lease_event_id = receipt.lease_event_id
     AND evidence.lease_event_sequence = receipt.lease_event_sequence
     AND evidence.finalized_at = receipt.accepted_at
    JOIN agent_control_initial_planning_stage_started started
      ON started.handoff_id = evidence.handoff_id
     AND started.handoff_fingerprint = evidence.handoff_fingerprint
     AND started.stage_run_id = evidence.stage_run_id
     AND started.attempt_id = evidence.attempt_id
     AND started.lease_id = evidence.lease_id
    WHERE receipt.finalization_command_id = ${row}.finalization_command_id
      AND receipt.result_evidence_id = ${row}.result_evidence_id
      AND receipt.handoff_id = ${row}.handoff_id
      AND receipt.accepted_at = ${row}.committed_at
      AND evidence.finalization_command_id = ${row}.finalization_command_id
      AND evidence.result_evidence_id = ${row}.result_evidence_id
      AND evidence.handoff_id = ${row}.handoff_id
      AND evidence.finalized_at = ${row}.committed_at
  )
`;

type LegacyEvidenceRow = Readonly<Record<string, string | number | null>>;
const legacyString = (row: LegacyEvidenceRow, column: string) => row[column] as string;
const legacyNumber = (row: LegacyEvidenceRow, column: string) => row[column] as number;

const legacyValidationError = (scope: string, detail: string) =>
  new Error(`migration 053 rejected legacy initial planning ${scope}: ${detail}`);

const validateLegacyCompanionFingerprints = Effect.fn(
  "validateLegacyInitialPlanningCompanionFingerprints",
)(function* (sql: SqlClient.SqlClient) {
  const startedRows = yield* sql<LegacyEvidenceRow>`
    SELECT * FROM agent_control_initial_planning_stage_started ORDER BY handoff_id
  `;
  for (const row of startedRows) {
    const handoffId = legacyString(row, "handoff_id");
    const handoffFingerprint = legacyString(row, "handoff_fingerprint");
    const binding = [
      handoffId,
      handoffFingerprint,
      legacyString(row, "project_id"),
      legacyString(row, "task_id"),
      String(legacyNumber(row, "task_revision")),
      String(legacyNumber(row, "github_intake_sequence")),
      legacyString(row, "source_identity_fingerprint"),
      legacyString(row, "controlled_thread_reservation_id"),
      legacyString(row, "thread_id"),
      legacyString(row, "stage_run_id"),
      legacyString(row, "attempt_id"),
      legacyString(row, "lease_id"),
      legacyString(row, "lease_holder_id"),
      String(legacyNumber(row, "fence_token")),
      legacyString(row, "provider_delivery_id"),
      legacyString(row, "provider_instance_id"),
      legacyString(row, "provider_turn_id"),
      legacyString(row, "runtime_mode"),
      legacyString(row, "model_selection_fingerprint"),
    ];
    const expectedFingerprint = fingerprintInitialPlanningFinalization("start", [
      ...binding,
      legacyString(row, "provider_accepted_at"),
      String(legacyNumber(row, "delivery_revision")),
      legacyString(row, "orchestration_started_event_id"),
      String(legacyNumber(row, "orchestration_started_sequence")),
      String(legacyNumber(row, "orchestration_started_stream_version")),
      legacyString(row, "stage_event_id"),
      String(legacyNumber(row, "stage_event_sequence")),
      String(legacyNumber(row, "stage_event_stream_version")),
      legacyString(row, "provider_accepted_at"),
    ]);
    if (
      legacyString(row, "start_command_id") !==
        deriveInitialPlanningStageStartCommandId(handoffId, handoffFingerprint) ||
      legacyString(row, "stage_event_id") !==
        deriveInitialPlanningStageStartedEventId(handoffId, handoffFingerprint) ||
      legacyString(row, "start_fingerprint") !== expectedFingerprint
    ) {
      return yield* Effect.die(
        legacyValidationError("stage-started evidence", "derived identity mismatch"),
      );
    }
  }

  const resultRows = yield* sql<LegacyEvidenceRow>`
    SELECT * FROM agent_control_initial_planning_result_evidence ORDER BY handoff_id
  `;
  for (const row of resultRows) {
    const handoffId = legacyString(row, "handoff_id");
    const handoffFingerprint = legacyString(row, "handoff_fingerprint");
    const proposedPlanJson = row.proposed_plan_json;
    const proposedPlanDigest = row.proposed_plan_digest;
    if (typeof proposedPlanJson === "string") {
      const canonicalPlan = yield* Effect.try({
        try: () => parseCanonicalJson(proposedPlanJson),
        catch: () => "noncanonical-proposed-plan" as const,
      }).pipe(
        Effect.catch(() =>
          Effect.die(legacyValidationError("result evidence", "noncanonical proposed plan JSON")),
        ),
      );
      if (
        canonicalPlan === null ||
        typeof canonicalPlan !== "object" ||
        Array.isArray(canonicalPlan) ||
        sha256Utf8(proposedPlanJson) !== proposedPlanDigest
      ) {
        return yield* Effect.die(
          legacyValidationError("result evidence", "proposed plan digest mismatch"),
        );
      }
    }
    const fingerprintParts = [
      handoffId,
      handoffFingerprint,
      legacyString(row, "project_id"),
      legacyString(row, "task_id"),
      String(legacyNumber(row, "task_revision")),
      String(legacyNumber(row, "github_intake_sequence")),
      legacyString(row, "source_identity_fingerprint"),
      legacyString(row, "controlled_thread_reservation_id"),
      legacyString(row, "thread_id"),
      legacyString(row, "stage_run_id"),
      legacyString(row, "attempt_id"),
      legacyString(row, "lease_id"),
      legacyString(row, "lease_holder_id"),
      String(legacyNumber(row, "fence_token")),
      legacyString(row, "provider_delivery_id"),
      legacyString(row, "provider_instance_id"),
      legacyString(row, "provider_turn_id"),
      legacyString(row, "runtime_mode"),
      legacyString(row, "model_selection_fingerprint"),
      legacyString(row, "outcome"),
      legacyString(row, "delivery_terminal_state"),
      String(legacyNumber(row, "delivery_revision")),
      legacyString(row, "terminal_at"),
      legacyString(row, "orchestration_started_event_id"),
      String(legacyNumber(row, "orchestration_started_sequence")),
      legacyString(row, "orchestration_terminal_event_id"),
      String(legacyNumber(row, "orchestration_terminal_sequence")),
      row.plan_id ?? "",
      row.plan_event_id ?? "",
      String(row.plan_event_sequence ?? 0),
      proposedPlanJson ?? "",
      proposedPlanDigest ?? "",
      legacyString(row, "stage_event_id"),
      String(legacyNumber(row, "stage_event_sequence")),
      String(legacyNumber(row, "stage_event_stream_version")),
      legacyString(row, "lease_event_id"),
      String(legacyNumber(row, "lease_event_sequence")),
      String(legacyNumber(row, "lease_event_stream_version")),
      legacyString(row, "finalized_at"),
    ].map(String);
    const expectedFingerprint = fingerprintInitialPlanningFinalization("result", fingerprintParts);
    if (
      legacyString(row, "finalization_command_id") !==
        deriveInitialPlanningFinalizationCommandId(handoffId, handoffFingerprint) ||
      legacyString(row, "result_evidence_id") !==
        deriveInitialPlanningResultEvidenceId(handoffId, handoffFingerprint) ||
      legacyString(row, "stage_event_id") !==
        deriveInitialPlanningTerminalStageEventId(handoffId, handoffFingerprint) ||
      legacyString(row, "lease_event_id") !==
        deriveInitialPlanningLeaseReleaseEventId(handoffId, handoffFingerprint) ||
      legacyString(row, "finalization_fingerprint") !== expectedFingerprint
    ) {
      return yield* Effect.die(
        legacyValidationError("result evidence", "derived identity mismatch"),
      );
    }
  }

  const markerRows = yield* sql<LegacyEvidenceRow>`
    SELECT marker.*, evidence.handoff_fingerprint,
      evidence.finalization_fingerprint, evidence.stage_event_id,
      evidence.stage_event_sequence, evidence.lease_event_id,
      evidence.lease_event_sequence, evidence.finalized_at
    FROM agent_control_initial_planning_finalization_markers marker
    JOIN agent_control_initial_planning_result_evidence evidence
      ON evidence.result_evidence_id = marker.result_evidence_id
    ORDER BY marker.handoff_id
  `;
  for (const row of markerRows) {
    const handoffId = legacyString(row, "handoff_id");
    const handoffFingerprint = legacyString(row, "handoff_fingerprint");
    const expectedMarkerFingerprint = fingerprintInitialPlanningFinalization("marker", [
      handoffId,
      handoffFingerprint,
      legacyString(row, "finalization_command_id"),
      legacyString(row, "result_evidence_id"),
      legacyString(row, "finalization_fingerprint"),
      legacyString(row, "stage_event_id"),
      String(legacyNumber(row, "stage_event_sequence")),
      legacyString(row, "lease_event_id"),
      String(legacyNumber(row, "lease_event_sequence")),
      legacyString(row, "finalized_at"),
    ]);
    if (
      legacyString(row, "marker_id") !==
        deriveInitialPlanningFinalizationMarkerId(handoffId, handoffFingerprint) ||
      legacyString(row, "marker_fingerprint") !== expectedMarkerFingerprint
    ) {
      return yield* Effect.die(
        legacyValidationError("finalization marker", "derived identity mismatch"),
      );
    }
  }
});

const validateLegacyInitialPlanningEvidence = Effect.fn("validateLegacyInitialPlanningEvidence")(
  function* () {
    const sql = yield* SqlClient.SqlClient;
    const foreignKeyViolations = yield* sql<{
      readonly table: string;
      readonly rowid: number | null;
      readonly parent: string;
      readonly fkid: number;
    }>`PRAGMA foreign_key_check`;
    if (foreignKeyViolations.length !== 0) {
      const coordinates = foreignKeyViolations
        .map(
          ({ table, rowid, parent, fkid }) =>
            `${table}[rowid=${String(rowid)},parent=${parent},fkid=${String(fkid)}]`,
        )
        .join(",");
      return yield* Effect.die(legacyValidationError("foreign keys", coordinates));
    }

    const [invalidLifecycle] = yield* sql.unsafe<{ readonly count: number }>(`
    SELECT count(*) AS count FROM agent_control_events event
    WHERE (
      event.event_type IN (
        'agentControl.stageRun.planningStarted',
        'agentControl.stageRun.planningSucceeded',
        'agentControl.stageRun.planningFailed',
        'agentControl.stageRun.planningCancelled'
      ) AND NOT (${stageEventPredicate("event")})
    ) OR (
      event.event_type = 'agentControl.stageRunLease.releasedAfterPlanning'
      AND NOT (${leaseEventPredicate("event")})
    )
  `);
    const [invalidStageStarted] = yield* sql.unsafe<{ readonly count: number }>(`
    SELECT count(*) AS count
    FROM agent_control_initial_planning_stage_started started
    WHERE NOT (${stageStartedPredicate("started", true)})
  `);
    const [invalidResults] = yield* sql.unsafe<{ readonly count: number }>(`
    SELECT count(*) AS count
    FROM agent_control_initial_planning_result_evidence evidence
    WHERE NOT (${resultEvidencePredicate("evidence")})
  `);
    const [invalidReceipts] = yield* sql.unsafe<{ readonly count: number }>(`
    SELECT count(*) AS count
    FROM agent_control_initial_planning_finalization_receipts receipt
    WHERE NOT (${receiptPredicate("receipt")})
  `);
    const [invalidMarkers] = yield* sql.unsafe<{ readonly count: number }>(`
    SELECT count(*) AS count
    FROM agent_control_initial_planning_finalization_markers marker
    WHERE NOT (${markerPredicate("marker")})
  `);
    const [invalidCompanionChains] = yield* sql.unsafe<{ readonly count: number }>(`
    SELECT count(*) AS count
    FROM (
      SELECT
        (SELECT count(*)
         FROM agent_control_initial_planning_result_evidence result
         WHERE result.handoff_id = started.handoff_id) AS result_count,
        (SELECT count(*)
         FROM agent_control_initial_planning_finalization_receipts receipt
         WHERE receipt.handoff_id = started.handoff_id) AS receipt_count,
        (SELECT count(*)
         FROM agent_control_initial_planning_finalization_markers marker
         WHERE marker.handoff_id = started.handoff_id) AS marker_count
      FROM agent_control_initial_planning_stage_started started
    ) chains
    WHERE NOT (
      (result_count = 0 AND receipt_count = 0 AND marker_count = 0)
      OR
      (result_count = 1 AND receipt_count = 1 AND marker_count = 1)
    )
  `);
    for (const [scope, count] of [
      ["lifecycle events", invalidLifecycle?.count],
      ["stage-started evidence", invalidStageStarted?.count],
      ["result evidence", invalidResults?.count],
      ["finalization receipts", invalidReceipts?.count],
      ["finalization markers", invalidMarkers?.count],
      ["companion chains", invalidCompanionChains?.count],
    ] as const) {
      if (count !== 0) {
        return yield* Effect.die(legacyValidationError(scope, `${String(count)} invalid row(s)`));
      }
    }
    yield* validateLegacyCompanionFingerprints(sql);
  },
);

export const hardenInitialPlanningStageFinalizationBoundary = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* validateLegacyInitialPlanningEvidence();
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_stage_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.event_type IN (
      'agentControl.stageRun.planningStarted',
      'agentControl.stageRun.planningSucceeded',
      'agentControl.stageRun.planningFailed',
      'agentControl.stageRun.planningCancelled'
    ) AND NOT (${stageEventPredicate("NEW")})
    BEGIN
      SELECT RAISE(ABORT, 'invalid initial planning stage lifecycle event');
    END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_lease_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.event_type = 'agentControl.stageRunLease.releasedAfterPlanning'
      AND NOT (${leaseEventPredicate("NEW")})
    BEGIN
      SELECT RAISE(ABORT, 'invalid initial planning lease lifecycle event');
    END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_lifecycle_event_no_update
    BEFORE UPDATE ON agent_control_events
    WHEN OLD.event_type IN (
      'agentControl.stageRun.planningStarted',
      'agentControl.stageRun.planningSucceeded',
      'agentControl.stageRun.planningFailed',
      'agentControl.stageRun.planningCancelled',
      'agentControl.stageRunLease.releasedAfterPlanning'
    )
    BEGIN
      SELECT RAISE(ABORT, 'initial planning lifecycle evidence is immutable');
    END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_lifecycle_event_no_delete
    BEFORE DELETE ON agent_control_events
    WHEN OLD.event_type IN (
      'agentControl.stageRun.planningStarted',
      'agentControl.stageRun.planningSucceeded',
      'agentControl.stageRun.planningFailed',
      'agentControl.stageRun.planningCancelled',
      'agentControl.stageRunLease.releasedAfterPlanning'
    )
    BEGIN
      SELECT RAISE(ABORT, 'initial planning lifecycle evidence is immutable');
    END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_stage_started_validate
    BEFORE INSERT ON agent_control_initial_planning_stage_started
    WHEN NOT (${stageStartedPredicate("NEW")})
    BEGIN
      SELECT RAISE(ABORT, 'initial planning start evidence is inconsistent');
    END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_result_evidence_validate
    BEFORE INSERT ON agent_control_initial_planning_result_evidence
    WHEN NOT (${resultEvidencePredicate("NEW")})
    BEGIN
      SELECT RAISE(ABORT, 'initial planning result evidence is inconsistent');
    END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_finalization_receipt_validate
    BEFORE INSERT ON agent_control_initial_planning_finalization_receipts
    WHEN NOT (${receiptPredicate("NEW")})
    BEGIN
      SELECT RAISE(ABORT, 'initial planning finalization receipt is inconsistent');
    END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_finalization_marker_validate
    BEFORE INSERT ON agent_control_initial_planning_finalization_markers
    WHEN NOT (${markerPredicate("NEW")})
    BEGIN
      SELECT RAISE(ABORT, 'initial planning finalization marker is inconsistent');
    END
  `).unprepared;
});

/**
 * Durable Initial Planning Stage finalization.
 *
 * The lifecycle events remain reconstructible from agent_control_events. The
 * four companion tables freeze provider-start, result, accepted-receipt, and
 * commit-marker coordinates without turning projections into authority.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const alreadyApplied = yield* sql<{ readonly count: number }>`
    SELECT count(*) AS count FROM sqlite_schema
    WHERE type = 'table'
      AND name = 'agent_control_initial_planning_finalization_markers'
  `;
  if (alreadyApplied[0]?.count === 1) {
    yield* hardenInitialPlanningStageFinalizationBoundary;
    return;
  }

  const eventTriggers = yield* sql<{
    readonly name: string;
    readonly sql: string;
  }>`
    SELECT name, sql FROM sqlite_schema
    WHERE type = 'trigger' AND sql IS NOT NULL
      AND (tbl_name = 'agent_control_events' OR sql LIKE '%agent_control_events%')
    ORDER BY name ASC
  `;
  const sequenceRows = yield* sql<{ readonly seq: number }>`
    SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
  `;
  const eventSequence = sequenceRows[0]?.seq;

  yield* sql`PRAGMA defer_foreign_keys = ON`;
  for (const trigger of eventTriggers) {
    yield* sql.unsafe(`DROP TRIGGER ${quoteSqliteIdentifier(trigger.name)}`).unprepared;
  }
  yield* sql`
    CREATE TABLE agent_control_events_rebuild_052 (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind IN (
        'project-controller', 'github-intake', 'task', 'stage-run',
        'stage-run-lease', 'worktree-reservation',
        'controlled-thread-reservation'
      )),
      stream_id TEXT NOT NULL,
      stream_version INTEGER NOT NULL CHECK (stream_version >= 1),
      event_type TEXT NOT NULL,
      occurred_at TEXT NOT NULL,
      command_id TEXT NOT NULL,
      causation_event_id TEXT,
      correlation_id TEXT NOT NULL,
      actor_authority TEXT NOT NULL CHECK (actor_authority IN ('human', 'controller', 'system')),
      payload_json TEXT NOT NULL,
      metadata_json TEXT NOT NULL,
      CHECK (
        (aggregate_kind = 'project-controller' AND event_type = 'agentControl.project.mode.changed')
        OR (aggregate_kind = 'github-intake' AND event_type IN (
          'agentControl.github.config.set', 'agentControl.github.config.cleared',
          'agentControl.github.poll.succeeded', 'agentControl.github.poll.failed'
        ))
        OR (aggregate_kind = 'task' AND event_type IN (
          'agentControl.task.created', 'agentControl.task.sourceGate.changed',
          'agentControl.task.needsAttentionMarked', 'agentControl.task.sourceMissingRecovered'
        ))
        OR (aggregate_kind = 'stage-run' AND event_type IN (
          'agentControl.stageRun.prepared',
          'agentControl.stageRun.planningStarted',
          'agentControl.stageRun.planningSucceeded',
          'agentControl.stageRun.planningFailed',
          'agentControl.stageRun.planningCancelled'
        ))
        OR (aggregate_kind = 'stage-run-lease' AND event_type IN (
          'agentControl.stageRunLease.reserved', 'agentControl.stageRunLease.renewed',
          'agentControl.stageRunLease.releasedBeforeExecution',
          'agentControl.stageRunLease.releasedAfterPlanning'
        ))
        OR (aggregate_kind = 'worktree-reservation' AND event_type IN (
          'agentControl.worktree.reserved',
          'agentControl.worktree.materializationStarted',
          'agentControl.worktree.ready',
          'agentControl.worktree.needsAttention'
        ))
        OR (
          aggregate_kind = 'controlled-thread-reservation'
          AND actor_authority = 'controller'
          AND (
            (stream_version = 1
              AND event_type = 'agentControl.controlledThreadReservation.prepared')
            OR (stream_version = 2
              AND event_type = 'agentControl.controlledThreadReservation.materializing')
            OR (stream_version = 3
              AND event_type = 'agentControl.controlledThreadReservation.bound')
          )
        )
      )
    )
  `;
  yield* sql`
    INSERT INTO agent_control_events_rebuild_052 (
      sequence, event_id, aggregate_kind, stream_id, stream_version,
      event_type, occurred_at, command_id, causation_event_id,
      correlation_id, actor_authority, payload_json, metadata_json
    )
    SELECT sequence, event_id, aggregate_kind, stream_id, stream_version,
      event_type, occurred_at, command_id, causation_event_id,
      correlation_id, actor_authority, payload_json, metadata_json
    FROM agent_control_events
  `;
  yield* sql`DROP TABLE agent_control_events`;
  yield* sql`ALTER TABLE agent_control_events_rebuild_052 RENAME TO agent_control_events`;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_stream_version
    ON agent_control_events(aggregate_kind, stream_id, stream_version)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_events_stream_sequence
    ON agent_control_events(aggregate_kind, stream_id, sequence)
  `;
  yield* sql`CREATE INDEX idx_agent_control_events_command_id ON agent_control_events(command_id)`;
  yield* sql`
    CREATE INDEX idx_agent_control_events_correlation_id
    ON agent_control_events(correlation_id)
  `;
  yield* sql`CREATE INDEX idx_agent_control_events_sequence ON agent_control_events(sequence)`;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_worktree_relational_identity
    ON agent_control_events(event_id, stream_id, stream_version, event_type)
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_controlled_thread_relational_identity
    ON agent_control_events(
      event_id, aggregate_kind, stream_id, stream_version, event_type, command_id
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_initial_planning_relational_identity
    ON agent_control_events(event_id, stream_id, stream_version)
  `;
  if (eventSequence !== undefined) {
    yield* sql`
      DELETE FROM sqlite_sequence
      WHERE name IN ('agent_control_events', 'agent_control_events_rebuild_052')
    `;
    yield* sql`
      INSERT INTO sqlite_sequence(name, seq) VALUES ('agent_control_events', ${eventSequence})
    `;
  }
  for (const trigger of eventTriggers) {
    yield* sql.unsafe(trigger.sql).unprepared;
  }

  yield* sql`
    CREATE TABLE agent_control_initial_planning_stage_started (
      start_command_id TEXT PRIMARY KEY CHECK (${sql.literal(text("start_command_id"))}),
      start_fingerprint TEXT UNIQUE NOT NULL CHECK (${sql.literal(sha256("start_fingerprint"))}),
      handoff_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("handoff_id"))}),
      handoff_fingerprint TEXT UNIQUE NOT NULL CHECK (${sql.literal(sha256("handoff_fingerprint"))}),
      project_id TEXT NOT NULL CHECK (${sql.literal(text("project_id"))}),
      task_id TEXT NOT NULL CHECK (${sql.literal(text("task_id"))}),
      task_revision INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("task_revision"))}),
      github_intake_sequence INTEGER NOT NULL CHECK (
        ${sql.literal(positiveInteger("github_intake_sequence"))}
      ),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        ${sql.literal(sha256("source_identity_fingerprint"))}
      ),
      controlled_thread_reservation_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("controlled_thread_reservation_id"))}
      ),
      thread_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      stage_run_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("stage_run_id"))}),
      attempt_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("attempt_id"))}),
      lease_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("lease_id"))}),
      lease_holder_id TEXT NOT NULL CHECK (${sql.literal(text("lease_holder_id"))}),
      fence_token INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("fence_token"))}),
      provider_delivery_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("provider_delivery_id"))}),
      provider_instance_id TEXT NOT NULL CHECK (${sql.literal(text("provider_instance_id"))}),
      provider_turn_id TEXT NOT NULL CHECK (${sql.literal(text("provider_turn_id"))}),
      runtime_mode TEXT NOT NULL CHECK (
        runtime_mode IN ('approval-required', 'full-access')
      ),
      model_selection_fingerprint TEXT NOT NULL CHECK (
        ${sql.literal(sha256("model_selection_fingerprint"))}
      ),
      provider_accepted_at TEXT NOT NULL CHECK (${sql.literal(timestamp("provider_accepted_at"))}),
      delivery_revision INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("delivery_revision"))}),
      orchestration_started_event_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("orchestration_started_event_id"))}
      ),
      orchestration_started_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("orchestration_started_sequence"))}
      ),
      orchestration_started_stream_version INTEGER NOT NULL CHECK (
        typeof(orchestration_started_stream_version) = 'integer'
        AND orchestration_started_stream_version >= 0
      ),
      stage_event_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("stage_event_id"))}),
      stage_event_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("stage_event_sequence"))}
      ),
      stage_event_stream_version INTEGER NOT NULL CHECK (stage_event_stream_version = 2),
      recorded_at TEXT NOT NULL CHECK (${sql.literal(timestamp("recorded_at"))}),
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_initial_planning_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (provider_delivery_id)
        REFERENCES agent_control_initial_planning_deliveries(provider_delivery_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (orchestration_started_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (
        stage_event_id, stage_run_id, stage_event_stream_version
      ) REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_result_evidence (
      result_evidence_id TEXT PRIMARY KEY CHECK (${sql.literal(text("result_evidence_id"))}),
      finalization_command_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("finalization_command_id"))}
      ),
      finalization_fingerprint TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("finalization_fingerprint"))}
      ),
      outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'cancelled')),
      handoff_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("handoff_id"))}),
      handoff_fingerprint TEXT UNIQUE NOT NULL CHECK (${sql.literal(sha256("handoff_fingerprint"))}),
      project_id TEXT NOT NULL CHECK (${sql.literal(text("project_id"))}),
      task_id TEXT NOT NULL CHECK (${sql.literal(text("task_id"))}),
      task_revision INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("task_revision"))}),
      github_intake_sequence INTEGER NOT NULL CHECK (
        ${sql.literal(positiveInteger("github_intake_sequence"))}
      ),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        ${sql.literal(sha256("source_identity_fingerprint"))}
      ),
      controlled_thread_reservation_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("controlled_thread_reservation_id"))}
      ),
      thread_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      stage_run_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("stage_run_id"))}),
      attempt_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("attempt_id"))}),
      lease_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("lease_id"))}),
      lease_holder_id TEXT NOT NULL CHECK (${sql.literal(text("lease_holder_id"))}),
      fence_token INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("fence_token"))}),
      provider_delivery_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("provider_delivery_id"))}),
      provider_instance_id TEXT NOT NULL CHECK (${sql.literal(text("provider_instance_id"))}),
      provider_turn_id TEXT NOT NULL CHECK (${sql.literal(text("provider_turn_id"))}),
      runtime_mode TEXT NOT NULL CHECK (runtime_mode IN ('approval-required', 'full-access')),
      model_selection_fingerprint TEXT NOT NULL CHECK (
        ${sql.literal(sha256("model_selection_fingerprint"))}
      ),
      delivery_terminal_state TEXT NOT NULL CHECK (
        delivery_terminal_state IN ('completed', 'failed', 'interrupted')
      ),
      delivery_revision INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("delivery_revision"))}),
      terminal_at TEXT NOT NULL CHECK (${sql.literal(timestamp("terminal_at"))}),
      orchestration_started_event_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("orchestration_started_event_id"))}
      ),
      orchestration_started_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("orchestration_started_sequence"))}
      ),
      orchestration_terminal_event_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("orchestration_terminal_event_id"))}
      ),
      orchestration_terminal_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("orchestration_terminal_sequence"))}
      ),
      plan_id TEXT UNIQUE,
      plan_event_id TEXT UNIQUE,
      plan_event_sequence INTEGER UNIQUE,
      proposed_plan_json TEXT,
      proposed_plan_digest TEXT UNIQUE,
      stage_event_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("stage_event_id"))}),
      stage_event_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("stage_event_sequence"))}
      ),
      stage_event_stream_version INTEGER NOT NULL CHECK (stage_event_stream_version = 3),
      lease_event_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("lease_event_id"))}),
      lease_event_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("lease_event_sequence"))}
      ),
      lease_event_stream_version INTEGER NOT NULL CHECK (lease_event_stream_version >= 2),
      finalized_at TEXT NOT NULL CHECK (${sql.literal(timestamp("finalized_at"))}),
      CHECK (
        (outcome = 'succeeded'
          AND plan_id IS NOT NULL AND ${sql.literal(text("plan_id"))}
          AND plan_event_id IS NOT NULL AND ${sql.literal(text("plan_event_id"))}
          AND ${sql.literal(positiveInteger("plan_event_sequence"))}
          AND proposed_plan_json IS NOT NULL AND ${sql.literal(text("proposed_plan_json"))}
          AND json_valid(proposed_plan_json) = 1
          AND proposed_plan_digest IS NOT NULL AND ${sql.literal(sha256("proposed_plan_digest"))})
        OR
        (outcome IN ('failed', 'cancelled')
          AND plan_id IS NULL AND plan_event_id IS NULL AND plan_event_sequence IS NULL
          AND proposed_plan_json IS NULL AND proposed_plan_digest IS NULL)
      ),
      CHECK (
        (delivery_terminal_state = 'completed' AND outcome = 'succeeded')
        OR (delivery_terminal_state = 'failed' AND outcome = 'failed')
        OR (delivery_terminal_state = 'interrupted' AND outcome = 'cancelled')
      ),
      CHECK (orchestration_terminal_sequence > orchestration_started_sequence),
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_initial_planning_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (provider_delivery_id)
        REFERENCES agent_control_initial_planning_deliveries(provider_delivery_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (orchestration_started_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (orchestration_terminal_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (plan_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (stage_event_id, stage_run_id, stage_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (lease_event_id, lease_id, lease_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_finalization_receipts (
      finalization_command_id TEXT PRIMARY KEY CHECK (
        ${sql.literal(text("finalization_command_id"))}
      ),
      finalization_fingerprint TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("finalization_fingerprint"))}
      ),
      result_evidence_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("result_evidence_id"))}),
      handoff_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("handoff_id"))}),
      outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'cancelled')),
      stage_event_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("stage_event_id"))}),
      stage_event_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("stage_event_sequence"))}
      ),
      lease_event_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("lease_event_id"))}),
      lease_event_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("lease_event_sequence"))}
      ),
      accepted_at TEXT NOT NULL CHECK (${sql.literal(timestamp("accepted_at"))}),
      FOREIGN KEY (result_evidence_id)
        REFERENCES agent_control_initial_planning_result_evidence(result_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_finalization_markers (
      marker_id TEXT PRIMARY KEY CHECK (${sql.literal(text("marker_id"))}),
      marker_fingerprint TEXT UNIQUE NOT NULL CHECK (${sql.literal(sha256("marker_fingerprint"))}),
      finalization_command_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("finalization_command_id"))}
      ),
      result_evidence_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("result_evidence_id"))}),
      handoff_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("handoff_id"))}),
      committed_at TEXT NOT NULL CHECK (${sql.literal(timestamp("committed_at"))}),
      FOREIGN KEY (finalization_command_id)
        REFERENCES agent_control_initial_planning_finalization_receipts(finalization_command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (result_evidence_id)
        REFERENCES agent_control_initial_planning_result_evidence(result_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  for (const table of [
    "agent_control_initial_planning_stage_started",
    "agent_control_initial_planning_result_evidence",
    "agent_control_initial_planning_finalization_receipts",
    "agent_control_initial_planning_finalization_markers",
  ] as const) {
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
       BEGIN SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable'); END`,
    ).unprepared;
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table}
       BEGIN SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable'); END`,
    ).unprepared;
  }

  yield* hardenInitialPlanningStageFinalizationBoundary;

  const foreignKeyViolations = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
  if (foreignKeyViolations.length !== 0) {
    return yield* Effect.die(new Error("migration 052 introduced foreign-key violations"));
  }
  yield* sql`PRAGMA defer_foreign_keys = OFF`;
});
