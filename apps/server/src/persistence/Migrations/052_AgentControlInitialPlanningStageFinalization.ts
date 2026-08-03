import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

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

export const hardenInitialPlanningStageFinalizationBoundary = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
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
  if (invalidLifecycle?.count !== 0) {
    return yield* Effect.die(
      new Error("existing initial planning lifecycle evidence violates the hardened boundary"),
    );
  }
  const [incompleteChain] = yield* sql<{
    readonly count: number;
  }>`
    SELECT (
      SELECT count(*)
      FROM agent_control_initial_planning_result_evidence evidence
      LEFT JOIN agent_control_initial_planning_finalization_receipts receipt
        ON receipt.finalization_command_id = evidence.finalization_command_id
       AND receipt.finalization_fingerprint = evidence.finalization_fingerprint
       AND receipt.result_evidence_id = evidence.result_evidence_id
       AND receipt.handoff_id = evidence.handoff_id
       AND receipt.outcome = evidence.outcome
       AND receipt.stage_event_id = evidence.stage_event_id
       AND receipt.stage_event_sequence = evidence.stage_event_sequence
       AND receipt.lease_event_id = evidence.lease_event_id
       AND receipt.lease_event_sequence = evidence.lease_event_sequence
       AND receipt.accepted_at = evidence.finalized_at
      LEFT JOIN agent_control_initial_planning_finalization_markers marker
        ON marker.finalization_command_id = evidence.finalization_command_id
       AND marker.result_evidence_id = evidence.result_evidence_id
       AND marker.handoff_id = evidence.handoff_id
       AND marker.committed_at = evidence.finalized_at
      WHERE receipt.finalization_command_id IS NULL OR marker.marker_id IS NULL
    ) + (
      SELECT count(*)
      FROM agent_control_initial_planning_finalization_receipts receipt
      LEFT JOIN agent_control_initial_planning_result_evidence evidence
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
      WHERE evidence.result_evidence_id IS NULL
    ) + (
      SELECT count(*)
      FROM agent_control_initial_planning_finalization_markers marker
      LEFT JOIN agent_control_initial_planning_finalization_receipts receipt
        ON receipt.finalization_command_id = marker.finalization_command_id
       AND receipt.result_evidence_id = marker.result_evidence_id
       AND receipt.handoff_id = marker.handoff_id
       AND receipt.accepted_at = marker.committed_at
      LEFT JOIN agent_control_initial_planning_result_evidence evidence
        ON evidence.finalization_command_id = marker.finalization_command_id
       AND evidence.result_evidence_id = marker.result_evidence_id
       AND evidence.handoff_id = marker.handoff_id
       AND evidence.finalized_at = marker.committed_at
      LEFT JOIN agent_control_initial_planning_stage_started started
        ON started.handoff_id = marker.handoff_id
      WHERE receipt.finalization_command_id IS NULL
         OR evidence.result_evidence_id IS NULL
         OR started.handoff_id IS NULL
    ) AS count
  `;
  if (incompleteChain?.count !== 0) {
    return yield* Effect.die(
      new Error("existing initial planning finalization rows form an incomplete evidence chain"),
    );
  }
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
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_events event
      JOIN agent_control_initial_planning_handoff_intents intent
        ON intent.handoff_id = NEW.handoff_id
      JOIN agent_control_initial_planning_handoff_accepted accepted
        ON accepted.handoff_id = intent.handoff_id
      JOIN agent_control_initial_planning_deliveries delivery
        ON delivery.handoff_id = intent.handoff_id
      JOIN agent_control_initial_planning_delivery_attestations attestation
        ON attestation.provider_delivery_id = delivery.provider_delivery_id
      JOIN orchestration_events orchestration
        ON orchestration.event_id = NEW.orchestration_started_event_id
      JOIN agent_control_stage_run_states stage_state
        ON stage_state.stage_run_id = NEW.stage_run_id
      JOIN agent_control_stage_run_lease_states lease_state
        ON lease_state.lease_id = NEW.lease_id
      WHERE event.event_id = NEW.stage_event_id
        AND event.sequence = NEW.stage_event_sequence
        AND event.stream_id = NEW.stage_run_id
        AND event.stream_version = NEW.stage_event_stream_version
        AND event.event_type = 'agentControl.stageRun.planningStarted'
        AND event.command_id = NEW.start_command_id
        AND json_extract(event.payload_json, '$.handoffId') = NEW.handoff_id
        AND json_extract(event.payload_json, '$.handoffFingerprint') = NEW.handoff_fingerprint
        AND json_extract(event.payload_json, '$.projectId') = NEW.project_id
        AND json_extract(event.payload_json, '$.taskId') = NEW.task_id
        AND json_extract(event.payload_json, '$.taskRevision') = NEW.task_revision
        AND json_extract(event.payload_json, '$.githubIntakeSequence') = NEW.github_intake_sequence
        AND json_extract(event.payload_json, '$.sourceIdentityFingerprint') =
          NEW.source_identity_fingerprint
        AND json_extract(event.payload_json, '$.controlledThreadReservationId') =
          NEW.controlled_thread_reservation_id
        AND json_extract(event.payload_json, '$.threadId') = NEW.thread_id
        AND json_extract(event.payload_json, '$.stageRunId') = NEW.stage_run_id
        AND json_extract(event.payload_json, '$.attemptId') = NEW.attempt_id
        AND json_extract(event.payload_json, '$.leaseId') = NEW.lease_id
        AND json_extract(event.payload_json, '$.leaseHolderId') = NEW.lease_holder_id
        AND json_extract(event.payload_json, '$.fenceToken') = NEW.fence_token
        AND json_extract(event.payload_json, '$.providerDeliveryId') = NEW.provider_delivery_id
        AND json_extract(event.payload_json, '$.providerInstanceId') = NEW.provider_instance_id
        AND json_extract(event.payload_json, '$.providerTurnId') = NEW.provider_turn_id
        AND json_extract(event.payload_json, '$.runtimeMode') = NEW.runtime_mode
        AND json_extract(event.payload_json, '$.modelSelectionFingerprint') =
          NEW.model_selection_fingerprint
        AND json_extract(event.payload_json, '$.startedAt') = NEW.provider_accepted_at
        AND intent.handoff_fingerprint = NEW.handoff_fingerprint
        AND intent.project_id = NEW.project_id AND intent.task_id = NEW.task_id
        AND intent.task_revision = NEW.task_revision
        AND intent.github_intake_sequence = NEW.github_intake_sequence
        AND intent.source_identity_fingerprint = NEW.source_identity_fingerprint
        AND intent.controlled_thread_reservation_id = NEW.controlled_thread_reservation_id
        AND intent.thread_id = NEW.thread_id AND intent.stage_run_id = NEW.stage_run_id
        AND intent.attempt_id = NEW.attempt_id AND intent.lease_id = NEW.lease_id
        AND intent.lease_holder_id = NEW.lease_holder_id
        AND intent.fence_token = NEW.fence_token
        AND delivery.provider_delivery_id = NEW.provider_delivery_id
        AND delivery.provider_instance_id = NEW.provider_instance_id
        AND delivery.provider_turn_id = NEW.provider_turn_id
        AND delivery.provider_accepted_at = NEW.provider_accepted_at
        AND delivery.revision = NEW.delivery_revision
        AND intent.runtime_mode = NEW.runtime_mode
        AND attestation.provider_instance_id = NEW.provider_instance_id
        AND attestation.model_selection_fingerprint = NEW.model_selection_fingerprint
        AND orchestration.sequence = NEW.orchestration_started_sequence
        AND orchestration.stream_version = NEW.orchestration_started_stream_version
        AND orchestration.stream_id = NEW.thread_id
        AND stage_state.status = 'running' AND stage_state.revision = 2
        AND stage_state.last_event_sequence = NEW.stage_event_sequence
        AND lease_state.status = 'reserved'
        AND lease_state.project_id = NEW.project_id AND lease_state.task_id = NEW.task_id
        AND lease_state.stage_run_id = NEW.stage_run_id
        AND lease_state.attempt_id = NEW.attempt_id
        AND lease_state.holder_id = NEW.lease_holder_id
        AND lease_state.fence_token = NEW.fence_token
        AND NEW.recorded_at = NEW.provider_accepted_at
    )
    BEGIN
      SELECT RAISE(ABORT, 'initial planning start evidence is inconsistent');
    END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_result_evidence_validate
    BEFORE INSERT ON agent_control_initial_planning_result_evidence
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_initial_planning_stage_started started
      JOIN agent_control_initial_planning_deliveries delivery
        ON delivery.handoff_id = started.handoff_id
      JOIN agent_control_events stage_event ON stage_event.event_id = NEW.stage_event_id
      JOIN agent_control_events lease_event ON lease_event.event_id = NEW.lease_event_id
      JOIN orchestration_events orchestration_started
        ON orchestration_started.event_id = NEW.orchestration_started_event_id
      JOIN orchestration_events orchestration_terminal
        ON orchestration_terminal.event_id = NEW.orchestration_terminal_event_id
      LEFT JOIN orchestration_events plan_event ON plan_event.event_id = NEW.plan_event_id
      JOIN agent_control_stage_run_states stage_state
        ON stage_state.stage_run_id = NEW.stage_run_id
      JOIN agent_control_stage_run_lease_states lease_state
        ON lease_state.lease_id = NEW.lease_id
      WHERE started.handoff_id = NEW.handoff_id
        AND started.handoff_fingerprint = NEW.handoff_fingerprint
        AND started.project_id = NEW.project_id AND started.task_id = NEW.task_id
        AND started.task_revision = NEW.task_revision
        AND started.github_intake_sequence = NEW.github_intake_sequence
        AND started.source_identity_fingerprint = NEW.source_identity_fingerprint
        AND started.controlled_thread_reservation_id = NEW.controlled_thread_reservation_id
        AND started.thread_id = NEW.thread_id AND started.stage_run_id = NEW.stage_run_id
        AND started.attempt_id = NEW.attempt_id AND started.lease_id = NEW.lease_id
        AND started.lease_holder_id = NEW.lease_holder_id
        AND started.fence_token = NEW.fence_token
        AND started.provider_delivery_id = NEW.provider_delivery_id
        AND started.provider_instance_id = NEW.provider_instance_id
        AND started.provider_turn_id = NEW.provider_turn_id
        AND started.runtime_mode = NEW.runtime_mode
        AND started.model_selection_fingerprint = NEW.model_selection_fingerprint
        AND delivery.state = NEW.delivery_terminal_state
        AND delivery.revision = NEW.delivery_revision
        AND delivery.terminal_at = NEW.terminal_at
        AND orchestration_started.sequence = NEW.orchestration_started_sequence
        AND orchestration_started.stream_id = NEW.thread_id
        AND orchestration_terminal.sequence = NEW.orchestration_terminal_sequence
        AND orchestration_terminal.stream_id = NEW.thread_id
        AND (
          (NEW.outcome = 'succeeded' AND plan_event.sequence = NEW.plan_event_sequence
            AND plan_event.stream_id = NEW.thread_id)
          OR (NEW.outcome IN ('failed', 'cancelled') AND plan_event.event_id IS NULL)
        )
        AND stage_event.sequence = NEW.stage_event_sequence
        AND stage_event.stream_id = NEW.stage_run_id
        AND stage_event.stream_version = NEW.stage_event_stream_version
        AND stage_event.command_id = NEW.finalization_command_id
        AND json_extract(stage_event.payload_json, '$.resultEvidenceId') = NEW.result_evidence_id
        AND json_extract(stage_event.payload_json, '$.status') = NEW.outcome
        AND json_extract(stage_event.payload_json, '$.finalizedAt') = NEW.finalized_at
        AND json_extract(stage_event.payload_json, '$.handoffId') = NEW.handoff_id
        AND json_extract(stage_event.payload_json, '$.handoffFingerprint') =
          NEW.handoff_fingerprint
        AND json_extract(stage_event.payload_json, '$.projectId') = NEW.project_id
        AND json_extract(stage_event.payload_json, '$.taskId') = NEW.task_id
        AND json_extract(stage_event.payload_json, '$.taskRevision') = NEW.task_revision
        AND json_extract(stage_event.payload_json, '$.githubIntakeSequence') =
          NEW.github_intake_sequence
        AND json_extract(stage_event.payload_json, '$.sourceIdentityFingerprint') =
          NEW.source_identity_fingerprint
        AND json_extract(stage_event.payload_json, '$.controlledThreadReservationId') =
          NEW.controlled_thread_reservation_id
        AND json_extract(stage_event.payload_json, '$.threadId') = NEW.thread_id
        AND json_extract(stage_event.payload_json, '$.stageRunId') = NEW.stage_run_id
        AND json_extract(stage_event.payload_json, '$.attemptId') = NEW.attempt_id
        AND json_extract(stage_event.payload_json, '$.leaseId') = NEW.lease_id
        AND json_extract(stage_event.payload_json, '$.leaseHolderId') = NEW.lease_holder_id
        AND json_extract(stage_event.payload_json, '$.fenceToken') = NEW.fence_token
        AND json_extract(stage_event.payload_json, '$.providerDeliveryId') =
          NEW.provider_delivery_id
        AND json_extract(stage_event.payload_json, '$.providerInstanceId') =
          NEW.provider_instance_id
        AND json_extract(stage_event.payload_json, '$.providerTurnId') = NEW.provider_turn_id
        AND json_extract(stage_event.payload_json, '$.runtimeMode') = NEW.runtime_mode
        AND json_extract(stage_event.payload_json, '$.modelSelectionFingerprint') =
          NEW.model_selection_fingerprint
        AND lease_event.sequence = NEW.lease_event_sequence
        AND lease_event.stream_id = NEW.lease_id
        AND lease_event.stream_version = NEW.lease_event_stream_version
        AND lease_event.command_id = NEW.finalization_command_id
        AND json_extract(lease_event.payload_json, '$.resultEvidenceId') = NEW.result_evidence_id
        AND json_extract(lease_event.payload_json, '$.stageStatus') = NEW.outcome
        AND json_extract(lease_event.payload_json, '$.releasedAt') = NEW.finalized_at
        AND json_extract(lease_event.payload_json, '$.handoffId') = NEW.handoff_id
        AND json_extract(lease_event.payload_json, '$.handoffFingerprint') =
          NEW.handoff_fingerprint
        AND json_extract(lease_event.payload_json, '$.projectId') = NEW.project_id
        AND json_extract(lease_event.payload_json, '$.taskId') = NEW.task_id
        AND json_extract(lease_event.payload_json, '$.taskRevision') = NEW.task_revision
        AND json_extract(lease_event.payload_json, '$.githubIntakeSequence') =
          NEW.github_intake_sequence
        AND json_extract(lease_event.payload_json, '$.sourceIdentityFingerprint') =
          NEW.source_identity_fingerprint
        AND json_extract(lease_event.payload_json, '$.controlledThreadReservationId') =
          NEW.controlled_thread_reservation_id
        AND json_extract(lease_event.payload_json, '$.threadId') = NEW.thread_id
        AND json_extract(lease_event.payload_json, '$.stageRunId') = NEW.stage_run_id
        AND json_extract(lease_event.payload_json, '$.attemptId') = NEW.attempt_id
        AND json_extract(lease_event.payload_json, '$.leaseId') = NEW.lease_id
        AND json_extract(lease_event.payload_json, '$.holderId') = NEW.lease_holder_id
        AND json_extract(lease_event.payload_json, '$.fenceToken') = NEW.fence_token
        AND json_extract(lease_event.payload_json, '$.providerDeliveryId') =
          NEW.provider_delivery_id
        AND json_extract(lease_event.payload_json, '$.providerInstanceId') =
          NEW.provider_instance_id
        AND json_extract(lease_event.payload_json, '$.providerTurnId') = NEW.provider_turn_id
        AND json_extract(lease_event.payload_json, '$.runtimeMode') = NEW.runtime_mode
        AND json_extract(lease_event.payload_json, '$.modelSelectionFingerprint') =
          NEW.model_selection_fingerprint
        AND stage_state.status = NEW.outcome
        AND stage_state.revision = NEW.stage_event_stream_version
        AND stage_state.last_event_sequence = NEW.stage_event_sequence
        AND lease_state.status = 'released'
        AND lease_state.revision = NEW.lease_event_stream_version
        AND lease_state.last_event_sequence = NEW.lease_event_sequence
        AND lease_state.released_at = NEW.finalized_at
        AND NEW.terminal_at = NEW.finalized_at
    )
    BEGIN
      SELECT RAISE(ABORT, 'initial planning result evidence is inconsistent');
    END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_finalization_receipt_validate
    BEFORE INSERT ON agent_control_initial_planning_finalization_receipts
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_initial_planning_result_evidence evidence
      WHERE evidence.finalization_command_id = NEW.finalization_command_id
        AND evidence.finalization_fingerprint = NEW.finalization_fingerprint
        AND evidence.result_evidence_id = NEW.result_evidence_id
        AND evidence.handoff_id = NEW.handoff_id
        AND evidence.outcome = NEW.outcome
        AND evidence.stage_event_id = NEW.stage_event_id
        AND evidence.stage_event_sequence = NEW.stage_event_sequence
        AND evidence.lease_event_id = NEW.lease_event_id
        AND evidence.lease_event_sequence = NEW.lease_event_sequence
        AND evidence.finalized_at = NEW.accepted_at
    )
    BEGIN
      SELECT RAISE(ABORT, 'initial planning finalization receipt is inconsistent');
    END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER IF NOT EXISTS agent_control_initial_planning_finalization_marker_validate
    BEFORE INSERT ON agent_control_initial_planning_finalization_markers
    WHEN NOT (
      ${text("NEW.marker_id")}
      AND ${sha256("NEW.marker_fingerprint")}
      AND EXISTS (
        SELECT 1
        FROM agent_control_initial_planning_finalization_receipts receipt
        JOIN agent_control_initial_planning_result_evidence evidence
          ON evidence.result_evidence_id = receipt.result_evidence_id
        JOIN agent_control_initial_planning_stage_started started
          ON started.handoff_id = evidence.handoff_id
        WHERE receipt.finalization_command_id = NEW.finalization_command_id
          AND receipt.result_evidence_id = NEW.result_evidence_id
          AND receipt.handoff_id = NEW.handoff_id
          AND evidence.finalization_command_id = NEW.finalization_command_id
          AND evidence.handoff_id = NEW.handoff_id
          AND evidence.finalized_at = NEW.committed_at
          AND receipt.accepted_at = NEW.committed_at
      )
    )
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
