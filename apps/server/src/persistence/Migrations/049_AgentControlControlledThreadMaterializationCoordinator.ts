import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const quoteSqliteIdentifier = (identifier: string): string =>
  `"${identifier.replaceAll('"', '""')}"`;

const controlledThreadTriggerNames = new Set([
  "agent_control_controlled_thread_catalog_no_update",
  "agent_control_controlled_thread_catalog_no_delete",
  "agent_control_controlled_thread_catalog_event_identity_insert",
  "agent_control_controlled_thread_event_validate",
  "agent_control_controlled_thread_event_json_total_validate",
  "agent_control_controlled_thread_event_no_update",
  "agent_control_controlled_thread_event_no_delete",
  "agent_control_controlled_thread_projection_validate_insert",
  "agent_control_controlled_thread_projection_validate_update",
  "agent_control_controlled_thread_projection_validate_update_json",
  "agent_control_controlled_thread_projection_json_total_validate_insert",
  "agent_control_controlled_thread_projection_json_total_validate_update",
]);

const reservationPayloadJsonTotalPredicate = `
  json_valid(NEW.payload_json) = 1
  AND json_type(NEW.payload_json) = 'object'
  AND json_valid(NEW.metadata_json) = 1
  AND json_type(NEW.metadata_json) = 'object'
  AND (SELECT count(*) FROM json_each(NEW.metadata_json)) = 1
  AND (SELECT count(DISTINCT key) FROM json_each(NEW.metadata_json)) = 1
  AND NOT EXISTS (
    SELECT 1 FROM json_each(NEW.metadata_json)
    WHERE key <> 'schemaVersion'
  )
  AND json_type(NEW.metadata_json, '$.schemaVersion') = 'integer'
  AND json_extract(NEW.metadata_json, '$.schemaVersion') = 1
  AND json_type(NEW.payload_json, '$.controlledThreadReservationId') = 'text'
  AND json_type(NEW.payload_json, '$.threadId') = 'text'
  AND json_type(NEW.payload_json, '$.projectId') = 'text'
  AND json_type(NEW.payload_json, '$.taskId') = 'text'
  AND json_type(NEW.payload_json, '$.taskRevision') = 'integer'
  AND json_extract(NEW.payload_json, '$.taskRevision') >= 1
  AND json_type(NEW.payload_json, '$.githubIntakeSequence') = 'integer'
  AND json_extract(NEW.payload_json, '$.githubIntakeSequence') >= 1
  AND json_type(NEW.payload_json, '$.sourceIdentityFingerprint') = 'text'
  AND length(json_extract(NEW.payload_json, '$.sourceIdentityFingerprint')) = 64
  AND json_extract(NEW.payload_json, '$.sourceIdentityFingerprint')
    NOT GLOB '*[^0-9a-f]*'
  AND json_type(NEW.payload_json, '$.stageRunId') = 'text'
  AND json_type(NEW.payload_json, '$.attemptId') = 'text'
  AND json_type(NEW.payload_json, '$.roleId') = 'text'
  AND json_extract(NEW.payload_json, '$.roleId') = 'planning'
  AND json_type(NEW.payload_json, '$.stageKind') = 'text'
  AND json_extract(NEW.payload_json, '$.stageKind') = 'planning'
  AND json_type(NEW.payload_json, '$.stageOrdinal') = 'integer'
  AND json_extract(NEW.payload_json, '$.stageOrdinal') = 1
  AND json_type(NEW.payload_json, '$.attemptOrdinal') = 'integer'
  AND json_extract(NEW.payload_json, '$.attemptOrdinal') = 1
  AND json_type(NEW.payload_json, '$.leaseId') = 'text'
  AND json_type(NEW.payload_json, '$.fenceToken') = 'integer'
  AND json_extract(NEW.payload_json, '$.fenceToken') >= 1
  AND json_type(NEW.payload_json, '$.worktreeReservationId') = 'text'
  AND json_type(NEW.payload_json, '$.status') = 'text'
  AND json_type(NEW.payload_json, '$.preparedAt') = 'text'
  AND length(json_extract(NEW.payload_json, '$.preparedAt')) = 24
  AND json_extract(NEW.payload_json, '$.preparedAt') GLOB
    '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
  AND (
    (
      NEW.stream_version = 1
      AND NEW.event_type = 'agentControl.controlledThreadReservation.prepared'
      AND json_extract(NEW.payload_json, '$.status') = 'prepared'
      AND (SELECT count(*) FROM json_each(NEW.payload_json)) = 18
      AND (SELECT count(DISTINCT key) FROM json_each(NEW.payload_json)) = 18
      AND NOT EXISTS (
        SELECT 1 FROM json_each(NEW.payload_json)
        WHERE key NOT IN (
          'controlledThreadReservationId', 'threadId', 'projectId', 'taskId',
          'taskRevision', 'githubIntakeSequence', 'sourceIdentityFingerprint',
          'stageRunId', 'attemptId', 'roleId', 'stageKind', 'stageOrdinal',
          'attemptOrdinal', 'leaseId', 'fenceToken', 'worktreeReservationId',
          'status', 'preparedAt'
        )
      )
    )
    OR (
      NEW.stream_version = 2
      AND NEW.event_type = 'agentControl.controlledThreadReservation.materializing'
      AND json_extract(NEW.payload_json, '$.status') = 'materializing'
      AND (SELECT count(*) FROM json_each(NEW.payload_json)) = 25
      AND (SELECT count(DISTINCT key) FROM json_each(NEW.payload_json)) = 25
      AND NOT EXISTS (
        SELECT 1 FROM json_each(NEW.payload_json)
        WHERE key NOT IN (
          'controlledThreadReservationId', 'threadId', 'projectId', 'taskId',
          'taskRevision', 'githubIntakeSequence', 'sourceIdentityFingerprint',
          'stageRunId', 'attemptId', 'roleId', 'stageKind', 'stageOrdinal',
          'attemptOrdinal', 'leaseId', 'fenceToken', 'worktreeReservationId',
          'status', 'preparedAt', 'coordinatorCommandId',
          'coordinatorCommandFingerprint', 'materializingTransitionCommandId',
          'materializationCommandId', 'materializationCommandFingerprint',
          'leaseHolderId', 'materializingAt'
        )
      )
      AND json_type(NEW.payload_json, '$.coordinatorCommandId') = 'text'
      AND json_type(NEW.payload_json, '$.coordinatorCommandFingerprint') = 'text'
      AND length(json_extract(NEW.payload_json, '$.coordinatorCommandFingerprint')) = 64
      AND json_extract(NEW.payload_json, '$.coordinatorCommandFingerprint')
        NOT GLOB '*[^0-9a-f]*'
      AND json_type(NEW.payload_json, '$.materializingTransitionCommandId') = 'text'
      AND json_type(NEW.payload_json, '$.materializationCommandId') = 'text'
      AND json_type(NEW.payload_json, '$.materializationCommandFingerprint') = 'text'
      AND length(json_extract(NEW.payload_json, '$.materializationCommandFingerprint')) = 64
      AND json_extract(NEW.payload_json, '$.materializationCommandFingerprint')
        NOT GLOB '*[^0-9a-f]*'
      AND json_type(NEW.payload_json, '$.leaseHolderId') = 'text'
      AND json_type(NEW.payload_json, '$.materializingAt') = 'text'
      AND length(json_extract(NEW.payload_json, '$.materializingAt')) = 24
      AND json_extract(NEW.payload_json, '$.materializingAt') GLOB
        '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
    )
    OR (
      NEW.stream_version = 3
      AND NEW.event_type = 'agentControl.controlledThreadReservation.bound'
      AND json_extract(NEW.payload_json, '$.status') = 'bound'
      AND (SELECT count(*) FROM json_each(NEW.payload_json)) = 29
      AND (SELECT count(DISTINCT key) FROM json_each(NEW.payload_json)) = 29
      AND NOT EXISTS (
        SELECT 1 FROM json_each(NEW.payload_json)
        WHERE key NOT IN (
          'controlledThreadReservationId', 'threadId', 'projectId', 'taskId',
          'taskRevision', 'githubIntakeSequence', 'sourceIdentityFingerprint',
          'stageRunId', 'attemptId', 'roleId', 'stageKind', 'stageOrdinal',
          'attemptOrdinal', 'leaseId', 'fenceToken', 'worktreeReservationId',
          'status', 'preparedAt', 'coordinatorCommandId',
          'coordinatorCommandFingerprint', 'materializingTransitionCommandId',
          'materializationCommandId', 'materializationCommandFingerprint',
          'leaseHolderId', 'materializingAt', 'boundTransitionCommandId',
          'orchestrationResultSequence', 'materializedAt', 'boundAt'
        )
      )
      AND json_type(NEW.payload_json, '$.coordinatorCommandId') = 'text'
      AND json_type(NEW.payload_json, '$.coordinatorCommandFingerprint') = 'text'
      AND length(json_extract(NEW.payload_json, '$.coordinatorCommandFingerprint')) = 64
      AND json_extract(NEW.payload_json, '$.coordinatorCommandFingerprint')
        NOT GLOB '*[^0-9a-f]*'
      AND json_type(NEW.payload_json, '$.materializingTransitionCommandId') = 'text'
      AND json_type(NEW.payload_json, '$.materializationCommandId') = 'text'
      AND json_type(NEW.payload_json, '$.materializationCommandFingerprint') = 'text'
      AND length(json_extract(NEW.payload_json, '$.materializationCommandFingerprint')) = 64
      AND json_extract(NEW.payload_json, '$.materializationCommandFingerprint')
        NOT GLOB '*[^0-9a-f]*'
      AND json_type(NEW.payload_json, '$.leaseHolderId') = 'text'
      AND json_type(NEW.payload_json, '$.materializingAt') = 'text'
      AND json_type(NEW.payload_json, '$.boundTransitionCommandId') = 'text'
      AND json_type(NEW.payload_json, '$.orchestrationResultSequence') = 'integer'
      AND json_extract(NEW.payload_json, '$.orchestrationResultSequence') >= 1
      AND json_type(NEW.payload_json, '$.materializedAt') = 'text'
      AND json_type(NEW.payload_json, '$.boundAt') = 'text'
      AND length(json_extract(NEW.payload_json, '$.materializingAt')) = 24
      AND length(json_extract(NEW.payload_json, '$.materializedAt')) = 24
      AND length(json_extract(NEW.payload_json, '$.boundAt')) = 24
      AND json_extract(NEW.payload_json, '$.materializingAt') GLOB
        '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
      AND json_extract(NEW.payload_json, '$.materializedAt') GLOB
        '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
      AND json_extract(NEW.payload_json, '$.boundAt') GLOB
        '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
    )
  )
`;

const reservationProjectionJsonTotalPredicate = `
  json_valid(NEW.state_json) = 1
  AND json_type(NEW.state_json) = 'object'
  AND json_type(NEW.state_json, '$.schemaVersion') = 'integer'
  AND json_extract(NEW.state_json, '$.schemaVersion') = 1
  AND json_type(NEW.state_json, '$.controlledThreadReservationId') = 'text'
  AND json_type(NEW.state_json, '$.threadId') = 'text'
  AND json_type(NEW.state_json, '$.projectId') = 'text'
  AND json_type(NEW.state_json, '$.taskId') = 'text'
  AND json_type(NEW.state_json, '$.taskRevision') = 'integer'
  AND json_type(NEW.state_json, '$.githubIntakeSequence') = 'integer'
  AND json_type(NEW.state_json, '$.sourceIdentityFingerprint') = 'text'
  AND json_type(NEW.state_json, '$.stageRunId') = 'text'
  AND json_type(NEW.state_json, '$.attemptId') = 'text'
  AND json_type(NEW.state_json, '$.roleId') = 'text'
  AND json_type(NEW.state_json, '$.stageKind') = 'text'
  AND json_type(NEW.state_json, '$.stageOrdinal') = 'integer'
  AND json_type(NEW.state_json, '$.attemptOrdinal') = 'integer'
  AND json_type(NEW.state_json, '$.leaseId') = 'text'
  AND json_type(NEW.state_json, '$.fenceToken') = 'integer'
  AND json_type(NEW.state_json, '$.worktreeReservationId') = 'text'
  AND json_type(NEW.state_json, '$.status') = 'text'
  AND json_type(NEW.state_json, '$.revision') = 'integer'
  AND json_type(NEW.state_json, '$.sequence') = 'integer'
  AND json_type(NEW.state_json, '$.preparedAt') = 'text'
  AND json_extract(NEW.state_json, '$.taskRevision') >= 1
  AND json_extract(NEW.state_json, '$.githubIntakeSequence') >= 1
  AND length(json_extract(NEW.state_json, '$.sourceIdentityFingerprint')) = 64
  AND json_extract(NEW.state_json, '$.sourceIdentityFingerprint')
    NOT GLOB '*[^0-9a-f]*'
  AND json_extract(NEW.state_json, '$.roleId') = 'planning'
  AND json_extract(NEW.state_json, '$.stageKind') = 'planning'
  AND json_extract(NEW.state_json, '$.stageOrdinal') = 1
  AND json_extract(NEW.state_json, '$.attemptOrdinal') = 1
  AND json_extract(NEW.state_json, '$.fenceToken') >= 1
  AND json_extract(NEW.state_json, '$.sequence') >= 1
  AND length(json_extract(NEW.state_json, '$.preparedAt')) = 24
  AND json_extract(NEW.state_json, '$.preparedAt') GLOB
    '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
  AND NEW.controlled_thread_reservation_id =
    json_extract(NEW.state_json, '$.controlledThreadReservationId')
  AND NEW.thread_id = json_extract(NEW.state_json, '$.threadId')
  AND NEW.project_id = json_extract(NEW.state_json, '$.projectId')
  AND NEW.task_id = json_extract(NEW.state_json, '$.taskId')
  AND NEW.task_revision = json_extract(NEW.state_json, '$.taskRevision')
  AND NEW.github_intake_sequence = json_extract(NEW.state_json, '$.githubIntakeSequence')
  AND NEW.source_identity_fingerprint =
    json_extract(NEW.state_json, '$.sourceIdentityFingerprint')
  AND NEW.stage_run_id = json_extract(NEW.state_json, '$.stageRunId')
  AND NEW.attempt_id = json_extract(NEW.state_json, '$.attemptId')
  AND NEW.role_id = json_extract(NEW.state_json, '$.roleId')
  AND NEW.stage_kind = json_extract(NEW.state_json, '$.stageKind')
  AND NEW.stage_ordinal = json_extract(NEW.state_json, '$.stageOrdinal')
  AND NEW.attempt_ordinal = json_extract(NEW.state_json, '$.attemptOrdinal')
  AND NEW.lease_id = json_extract(NEW.state_json, '$.leaseId')
  AND NEW.fence_token = json_extract(NEW.state_json, '$.fenceToken')
  AND NEW.worktree_reservation_id = json_extract(NEW.state_json, '$.worktreeReservationId')
  AND NEW.status = json_extract(NEW.state_json, '$.status')
  AND NEW.revision = json_extract(NEW.state_json, '$.revision')
  AND NEW.last_event_sequence = json_extract(NEW.state_json, '$.sequence')
  AND NEW.prepared_at = json_extract(NEW.state_json, '$.preparedAt')
  AND (
    (
      NEW.status = 'prepared' AND NEW.revision = 1
      AND (SELECT count(*) FROM json_each(NEW.state_json)) = 21
      AND (SELECT count(DISTINCT key) FROM json_each(NEW.state_json)) = 21
      AND NOT EXISTS (
        SELECT 1 FROM json_each(NEW.state_json)
        WHERE key NOT IN (
          'schemaVersion', 'controlledThreadReservationId', 'threadId',
          'projectId', 'taskId', 'taskRevision', 'githubIntakeSequence',
          'sourceIdentityFingerprint', 'stageRunId', 'attemptId', 'roleId',
          'stageKind', 'stageOrdinal', 'attemptOrdinal', 'leaseId',
          'fenceToken', 'worktreeReservationId', 'status', 'revision',
          'sequence', 'preparedAt'
        )
      )
      AND NEW.coordinator_command_id IS NULL
      AND NEW.coordinator_command_fingerprint IS NULL
      AND NEW.materializing_transition_command_id IS NULL
      AND NEW.materialization_command_id IS NULL
      AND NEW.materialization_command_fingerprint IS NULL
      AND NEW.lease_holder_id IS NULL
      AND NEW.materializing_at IS NULL
      AND NEW.bound_transition_command_id IS NULL
      AND NEW.orchestration_result_sequence IS NULL
      AND NEW.materialized_at IS NULL
      AND NEW.bound_at IS NULL
    )
    OR (
      NEW.status = 'materializing' AND NEW.revision = 2
      AND (SELECT count(*) FROM json_each(NEW.state_json)) = 28
      AND (SELECT count(DISTINCT key) FROM json_each(NEW.state_json)) = 28
      AND NOT EXISTS (
        SELECT 1 FROM json_each(NEW.state_json)
        WHERE key NOT IN (
          'schemaVersion', 'controlledThreadReservationId', 'threadId',
          'projectId', 'taskId', 'taskRevision', 'githubIntakeSequence',
          'sourceIdentityFingerprint', 'stageRunId', 'attemptId', 'roleId',
          'stageKind', 'stageOrdinal', 'attemptOrdinal', 'leaseId',
          'fenceToken', 'worktreeReservationId', 'status', 'revision',
          'sequence', 'preparedAt', 'coordinatorCommandId',
          'coordinatorCommandFingerprint', 'materializingTransitionCommandId',
          'materializationCommandId', 'materializationCommandFingerprint',
          'leaseHolderId', 'materializingAt'
        )
      )
      AND json_type(NEW.state_json, '$.coordinatorCommandId') = 'text'
      AND json_type(NEW.state_json, '$.coordinatorCommandFingerprint') = 'text'
      AND json_type(NEW.state_json, '$.materializingTransitionCommandId') = 'text'
      AND json_type(NEW.state_json, '$.materializationCommandId') = 'text'
      AND json_type(NEW.state_json, '$.materializationCommandFingerprint') = 'text'
      AND json_type(NEW.state_json, '$.leaseHolderId') = 'text'
      AND json_type(NEW.state_json, '$.materializingAt') = 'text'
      AND length(json_extract(NEW.state_json, '$.coordinatorCommandFingerprint')) = 64
      AND json_extract(NEW.state_json, '$.coordinatorCommandFingerprint')
        NOT GLOB '*[^0-9a-f]*'
      AND length(json_extract(NEW.state_json, '$.materializationCommandFingerprint')) = 64
      AND json_extract(NEW.state_json, '$.materializationCommandFingerprint')
        NOT GLOB '*[^0-9a-f]*'
      AND length(json_extract(NEW.state_json, '$.materializingAt')) = 24
      AND json_extract(NEW.state_json, '$.materializingAt') GLOB
        '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
      AND NEW.coordinator_command_id = json_extract(NEW.state_json, '$.coordinatorCommandId')
      AND NEW.coordinator_command_fingerprint =
        json_extract(NEW.state_json, '$.coordinatorCommandFingerprint')
      AND NEW.materializing_transition_command_id =
        json_extract(NEW.state_json, '$.materializingTransitionCommandId')
      AND NEW.materialization_command_id =
        json_extract(NEW.state_json, '$.materializationCommandId')
      AND NEW.materialization_command_fingerprint =
        json_extract(NEW.state_json, '$.materializationCommandFingerprint')
      AND NEW.lease_holder_id = json_extract(NEW.state_json, '$.leaseHolderId')
      AND NEW.materializing_at = json_extract(NEW.state_json, '$.materializingAt')
      AND NEW.bound_transition_command_id IS NULL
      AND NEW.orchestration_result_sequence IS NULL
      AND NEW.materialized_at IS NULL
      AND NEW.bound_at IS NULL
    )
    OR (
      NEW.status = 'bound' AND NEW.revision = 3
      AND (SELECT count(*) FROM json_each(NEW.state_json)) = 32
      AND (SELECT count(DISTINCT key) FROM json_each(NEW.state_json)) = 32
      AND NOT EXISTS (
        SELECT 1 FROM json_each(NEW.state_json)
        WHERE key NOT IN (
          'schemaVersion', 'controlledThreadReservationId', 'threadId',
          'projectId', 'taskId', 'taskRevision', 'githubIntakeSequence',
          'sourceIdentityFingerprint', 'stageRunId', 'attemptId', 'roleId',
          'stageKind', 'stageOrdinal', 'attemptOrdinal', 'leaseId',
          'fenceToken', 'worktreeReservationId', 'status', 'revision',
          'sequence', 'preparedAt', 'coordinatorCommandId',
          'coordinatorCommandFingerprint', 'materializingTransitionCommandId',
          'materializationCommandId', 'materializationCommandFingerprint',
          'leaseHolderId', 'materializingAt', 'boundTransitionCommandId',
          'orchestrationResultSequence', 'materializedAt', 'boundAt'
        )
      )
      AND json_type(NEW.state_json, '$.coordinatorCommandId') = 'text'
      AND json_type(NEW.state_json, '$.coordinatorCommandFingerprint') = 'text'
      AND json_type(NEW.state_json, '$.materializingTransitionCommandId') = 'text'
      AND json_type(NEW.state_json, '$.materializationCommandId') = 'text'
      AND json_type(NEW.state_json, '$.materializationCommandFingerprint') = 'text'
      AND json_type(NEW.state_json, '$.leaseHolderId') = 'text'
      AND json_type(NEW.state_json, '$.materializingAt') = 'text'
      AND json_type(NEW.state_json, '$.boundTransitionCommandId') = 'text'
      AND json_type(NEW.state_json, '$.orchestrationResultSequence') = 'integer'
      AND json_type(NEW.state_json, '$.materializedAt') = 'text'
      AND json_type(NEW.state_json, '$.boundAt') = 'text'
      AND length(json_extract(NEW.state_json, '$.coordinatorCommandFingerprint')) = 64
      AND json_extract(NEW.state_json, '$.coordinatorCommandFingerprint')
        NOT GLOB '*[^0-9a-f]*'
      AND length(json_extract(NEW.state_json, '$.materializationCommandFingerprint')) = 64
      AND json_extract(NEW.state_json, '$.materializationCommandFingerprint')
        NOT GLOB '*[^0-9a-f]*'
      AND json_extract(NEW.state_json, '$.orchestrationResultSequence') >= 1
      AND length(json_extract(NEW.state_json, '$.materializingAt')) = 24
      AND length(json_extract(NEW.state_json, '$.materializedAt')) = 24
      AND length(json_extract(NEW.state_json, '$.boundAt')) = 24
      AND json_extract(NEW.state_json, '$.materializingAt') GLOB
        '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
      AND json_extract(NEW.state_json, '$.materializedAt') GLOB
        '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
      AND json_extract(NEW.state_json, '$.boundAt') GLOB
        '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
      AND NEW.coordinator_command_id = json_extract(NEW.state_json, '$.coordinatorCommandId')
      AND NEW.coordinator_command_fingerprint =
        json_extract(NEW.state_json, '$.coordinatorCommandFingerprint')
      AND NEW.materializing_transition_command_id =
        json_extract(NEW.state_json, '$.materializingTransitionCommandId')
      AND NEW.materialization_command_id =
        json_extract(NEW.state_json, '$.materializationCommandId')
      AND NEW.materialization_command_fingerprint =
        json_extract(NEW.state_json, '$.materializationCommandFingerprint')
      AND NEW.lease_holder_id = json_extract(NEW.state_json, '$.leaseHolderId')
      AND NEW.materializing_at = json_extract(NEW.state_json, '$.materializingAt')
      AND NEW.bound_transition_command_id =
        json_extract(NEW.state_json, '$.boundTransitionCommandId')
      AND NEW.orchestration_result_sequence =
        json_extract(NEW.state_json, '$.orchestrationResultSequence')
      AND NEW.materialized_at = json_extract(NEW.state_json, '$.materializedAt')
      AND NEW.bound_at = json_extract(NEW.state_json, '$.boundAt')
    )
  )
`;

/**
 * Extends the reservation stream to prepared -> materializing -> bound and
 * adds immutable coordinator evidence. The accepted marker is deliberately
 * circularly coupled to the coordinator intent/receipt and is inserted last by
 * the coordinator transaction.
 */
export default Effect.suspend(() =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const alreadyApplied = yield* sql<{ readonly count: number }>`
    SELECT count(*) AS count
    FROM sqlite_schema
    WHERE type = 'table'
      AND name = 'agent_control_controlled_thread_materialization_accepted'
  `;
    if (alreadyApplied[0]?.count === 1) return;

    const dependentTriggers = yield* sql<{
      readonly name: string;
      readonly sql: string | null;
    }>`
    SELECT name, sql
    FROM sqlite_schema
    WHERE type = 'trigger'
      AND sql IS NOT NULL
      AND (
        sql LIKE '%agent_control_events%'
        OR sql LIKE '%agent_control_controlled_thread_stream_catalog%'
        OR sql LIKE '%agent_control_controlled_thread_reservation_states%'
        OR tbl_name = 'agent_control_worktree_event_envelopes'
      )
    ORDER BY name ASC
  `;
    const sequenceRows = yield* sql<{ readonly seq: number }>`
    SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
  `;
    const eventSequence = sequenceRows[0]?.seq;

    yield* sql`PRAGMA defer_foreign_keys = ON`;
    for (const trigger of dependentTriggers) {
      yield* sql.unsafe(`DROP TRIGGER ${quoteSqliteIdentifier(trigger.name)}`).unprepared;
    }
    yield* sql`
    CREATE TABLE agent_control_controlled_thread_stream_catalog_backup_049
    AS SELECT * FROM agent_control_controlled_thread_stream_catalog
  `;
    yield* sql`
    CREATE TABLE agent_control_worktree_event_envelopes_backup_049
    AS SELECT * FROM agent_control_worktree_event_envelopes
  `;
    yield* sql`DELETE FROM agent_control_worktree_event_envelopes`;
    yield* sql`DELETE FROM agent_control_controlled_thread_stream_catalog`;

    yield* sql`
    CREATE TABLE agent_control_events_rebuild_049 (
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
        OR (aggregate_kind = 'stage-run' AND event_type = 'agentControl.stageRun.prepared')
        OR (aggregate_kind = 'stage-run-lease' AND event_type IN (
          'agentControl.stageRunLease.reserved', 'agentControl.stageRunLease.renewed',
          'agentControl.stageRunLease.releasedBeforeExecution'
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
            (
              stream_version = 1
              AND event_type = 'agentControl.controlledThreadReservation.prepared'
            )
            OR (
              stream_version = 2
              AND event_type = 'agentControl.controlledThreadReservation.materializing'
            )
            OR (
              stream_version = 3
              AND event_type = 'agentControl.controlledThreadReservation.bound'
            )
          )
        )
      )
    )
  `;
    yield* sql`
    INSERT INTO agent_control_events_rebuild_049 (
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
    yield* sql`ALTER TABLE agent_control_events_rebuild_049 RENAME TO agent_control_events`;
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
    if (eventSequence !== undefined) {
      yield* sql`
      DELETE FROM sqlite_sequence
      WHERE name IN ('agent_control_events', 'agent_control_events_rebuild_049')
    `;
      yield* sql`
      INSERT INTO sqlite_sequence(name, seq)
      VALUES ('agent_control_events', ${eventSequence})
    `;
    }
    for (const triggerName of controlledThreadTriggerNames) {
      yield* sql.unsafe(`DROP TRIGGER IF EXISTS ${quoteSqliteIdentifier(triggerName)}`).unprepared;
    }
    yield* sql`
    ALTER TABLE agent_control_controlled_thread_stream_catalog
    RENAME TO agent_control_controlled_thread_stream_catalog_old_049
  `;
    yield* sql`
    ALTER TABLE agent_control_controlled_thread_reservation_states
    RENAME TO agent_control_controlled_thread_reservation_states_old_049
  `;

    yield* sql`
    CREATE TABLE agent_control_controlled_thread_reservation_states (
      controlled_thread_reservation_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL UNIQUE,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        length(source_identity_fingerprint) = 64
        AND source_identity_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      stage_run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      role_id TEXT NOT NULL CHECK (role_id = 'planning'),
      stage_kind TEXT NOT NULL CHECK (stage_kind = 'planning'),
      stage_ordinal INTEGER NOT NULL CHECK (stage_ordinal = 1),
      attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal = 1),
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 1),
      worktree_reservation_id TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('prepared', 'materializing', 'bound')),
      revision INTEGER NOT NULL CHECK (revision IN (1, 2, 3)),
      last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 1),
      prepared_at TEXT NOT NULL,
      coordinator_command_id TEXT,
      coordinator_command_fingerprint TEXT,
      materializing_transition_command_id TEXT,
      materialization_command_id TEXT,
      materialization_command_fingerprint TEXT,
      lease_holder_id TEXT,
      materializing_at TEXT,
      bound_transition_command_id TEXT,
      orchestration_result_sequence INTEGER CHECK (
        orchestration_result_sequence IS NULL OR orchestration_result_sequence >= 1
      ),
      materialized_at TEXT,
      bound_at TEXT,
      state_json TEXT NOT NULL CHECK (
        COALESCE(json_valid(state_json) = 1 AND json_type(state_json) = 'object', 0) = 1
      ),
      CHECK (
        COALESCE(
          (
            status = 'prepared' AND revision = 1
            AND coordinator_command_id IS NULL
            AND coordinator_command_fingerprint IS NULL
            AND materializing_transition_command_id IS NULL
            AND materialization_command_id IS NULL
            AND materialization_command_fingerprint IS NULL
            AND lease_holder_id IS NULL AND materializing_at IS NULL
            AND bound_transition_command_id IS NULL
            AND orchestration_result_sequence IS NULL
            AND materialized_at IS NULL AND bound_at IS NULL
          )
          OR (
            status = 'materializing' AND revision = 2
            AND coordinator_command_id IS NOT NULL
            AND length(coordinator_command_fingerprint) = 64
            AND coordinator_command_fingerprint NOT GLOB '*[^0-9a-f]*'
            AND materializing_transition_command_id IS NOT NULL
            AND materialization_command_id IS NOT NULL
            AND length(materialization_command_fingerprint) = 64
            AND materialization_command_fingerprint NOT GLOB '*[^0-9a-f]*'
            AND lease_holder_id IS NOT NULL AND materializing_at IS NOT NULL
            AND bound_transition_command_id IS NULL
            AND orchestration_result_sequence IS NULL
            AND materialized_at IS NULL AND bound_at IS NULL
          )
          OR (
            status = 'bound' AND revision = 3
            AND coordinator_command_id IS NOT NULL
            AND length(coordinator_command_fingerprint) = 64
            AND coordinator_command_fingerprint NOT GLOB '*[^0-9a-f]*'
            AND materializing_transition_command_id IS NOT NULL
            AND materialization_command_id IS NOT NULL
            AND length(materialization_command_fingerprint) = 64
            AND materialization_command_fingerprint NOT GLOB '*[^0-9a-f]*'
            AND lease_holder_id IS NOT NULL AND materializing_at IS NOT NULL
            AND bound_transition_command_id IS NOT NULL
            AND orchestration_result_sequence IS NOT NULL
            AND materialized_at IS NOT NULL AND bound_at IS NOT NULL
            AND materializing_at <= materialized_at
            AND materialized_at <= bound_at
          ),
          0
        ) = 1
      )
    )
  `;
    yield* sql`
    INSERT INTO agent_control_controlled_thread_reservation_states (
      controlled_thread_reservation_id, thread_id, project_id, task_id,
      task_revision, github_intake_sequence, source_identity_fingerprint,
      stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
      attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
      status, revision, last_event_sequence, prepared_at, state_json
    )
    SELECT controlled_thread_reservation_id, thread_id, project_id, task_id,
      task_revision, github_intake_sequence, source_identity_fingerprint,
      stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
      attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
      status, revision, last_event_sequence, prepared_at, state_json
    FROM agent_control_controlled_thread_reservation_states_old_049
  `;
    yield* sql`DROP TABLE agent_control_controlled_thread_reservation_states_old_049`;

    yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_controlled_thread_semantic_position
    ON agent_control_controlled_thread_reservation_states(
      project_id, task_id, stage_run_id, attempt_id, role_id,
      stage_ordinal, attempt_ordinal
    )
  `;
    yield* sql`
    CREATE INDEX idx_agent_control_controlled_thread_project_task
    ON agent_control_controlled_thread_reservation_states(
      project_id, task_id, task_revision, github_intake_sequence,
      controlled_thread_reservation_id
    )
  `;
    yield* sql`
    CREATE INDEX idx_agent_control_controlled_thread_stage
    ON agent_control_controlled_thread_reservation_states(
      project_id, task_id, stage_run_id, attempt_id
    )
  `;
    yield* sql`
    CREATE INDEX idx_agent_control_controlled_thread_thread
    ON agent_control_controlled_thread_reservation_states(thread_id)
  `;
    yield* sql`
    CREATE INDEX idx_agent_control_controlled_thread_worktree
    ON agent_control_controlled_thread_reservation_states(worktree_reservation_id)
  `;
    yield* sql`
    CREATE INDEX idx_agent_control_controlled_thread_sequence
    ON agent_control_controlled_thread_reservation_states(last_event_sequence)
  `;

    yield* sql`
    CREATE TABLE agent_control_controlled_thread_stream_catalog (
      controlled_thread_reservation_id TEXT NOT NULL,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL DEFAULT 'controlled-thread-reservation' CHECK (
        aggregate_kind = 'controlled-thread-reservation'
      ),
      stream_version INTEGER NOT NULL CHECK (stream_version IN (1, 2, 3)),
      command_id TEXT NOT NULL,
      event_type TEXT NOT NULL CHECK (event_type IN (
        'agentControl.controlledThreadReservation.prepared',
        'agentControl.controlledThreadReservation.materializing',
        'agentControl.controlledThreadReservation.bound'
      )),
      thread_id TEXT NOT NULL CHECK (thread_id LIKE 't3-auto-reserved-thread-%'),
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        length(source_identity_fingerprint) = 64
        AND source_identity_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      stage_run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      role_id TEXT NOT NULL CHECK (role_id = 'planning'),
      stage_kind TEXT NOT NULL CHECK (stage_kind = 'planning'),
      stage_ordinal INTEGER NOT NULL CHECK (stage_ordinal = 1),
      attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal = 1),
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 1),
      worktree_reservation_id TEXT NOT NULL,
      prepared_at TEXT NOT NULL,
      coordinator_command_id TEXT,
      coordinator_command_fingerprint TEXT,
      materializing_transition_command_id TEXT,
      materialization_command_id TEXT,
      materialization_command_fingerprint TEXT,
      lease_holder_id TEXT,
      materializing_at TEXT,
      bound_transition_command_id TEXT,
      orchestration_result_sequence INTEGER,
      materialized_at TEXT,
      bound_at TEXT,
      PRIMARY KEY (controlled_thread_reservation_id, stream_version),
      CHECK (
        COALESCE(
          (
            stream_version = 1
            AND event_type = 'agentControl.controlledThreadReservation.prepared'
            AND coordinator_command_id IS NULL
            AND coordinator_command_fingerprint IS NULL
            AND materializing_transition_command_id IS NULL
            AND materialization_command_id IS NULL
            AND materialization_command_fingerprint IS NULL
            AND lease_holder_id IS NULL AND materializing_at IS NULL
            AND bound_transition_command_id IS NULL
            AND orchestration_result_sequence IS NULL
            AND materialized_at IS NULL AND bound_at IS NULL
          )
          OR (
            stream_version = 2
            AND event_type = 'agentControl.controlledThreadReservation.materializing'
            AND coordinator_command_id IS NOT NULL
            AND length(coordinator_command_fingerprint) = 64
            AND coordinator_command_fingerprint NOT GLOB '*[^0-9a-f]*'
            AND materializing_transition_command_id IS command_id
            AND materialization_command_id IS NOT NULL
            AND length(materialization_command_fingerprint) = 64
            AND materialization_command_fingerprint NOT GLOB '*[^0-9a-f]*'
            AND lease_holder_id IS NOT NULL AND materializing_at IS NOT NULL
            AND bound_transition_command_id IS NULL
            AND orchestration_result_sequence IS NULL
            AND materialized_at IS NULL AND bound_at IS NULL
          )
          OR (
            stream_version = 3
            AND event_type = 'agentControl.controlledThreadReservation.bound'
            AND coordinator_command_id IS NOT NULL
            AND length(coordinator_command_fingerprint) = 64
            AND coordinator_command_fingerprint NOT GLOB '*[^0-9a-f]*'
            AND materializing_transition_command_id IS NOT NULL
            AND materialization_command_id IS NOT NULL
            AND length(materialization_command_fingerprint) = 64
            AND materialization_command_fingerprint NOT GLOB '*[^0-9a-f]*'
            AND lease_holder_id IS NOT NULL AND materializing_at IS NOT NULL
            AND bound_transition_command_id IS command_id
            AND orchestration_result_sequence >= 1
            AND materialized_at IS NOT NULL AND bound_at IS NOT NULL
            AND materializing_at <= materialized_at AND materialized_at <= bound_at
          ),
          0
        ) = 1
      ),
      FOREIGN KEY (
        event_id, aggregate_kind, controlled_thread_reservation_id,
        stream_version, event_type, command_id
      ) REFERENCES agent_control_events(
        event_id, aggregate_kind, stream_id,
        stream_version, event_type, command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;
    yield* sql`
    INSERT INTO agent_control_controlled_thread_stream_catalog (
      controlled_thread_reservation_id, event_id, aggregate_kind,
      stream_version, command_id, event_type, thread_id, project_id, task_id,
      task_revision, github_intake_sequence, source_identity_fingerprint,
      stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
      attempt_ordinal, lease_id, fence_token, worktree_reservation_id, prepared_at
    )
    SELECT controlled_thread_reservation_id, event_id, aggregate_kind,
      stream_version, command_id, event_type, thread_id, project_id, task_id,
      task_revision, github_intake_sequence, source_identity_fingerprint,
      stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
      attempt_ordinal, lease_id, fence_token, worktree_reservation_id, prepared_at
    FROM agent_control_controlled_thread_stream_catalog_backup_049
  `;
    yield* sql`DROP TABLE agent_control_controlled_thread_stream_catalog_old_049`;
    yield* sql`DROP TABLE agent_control_controlled_thread_stream_catalog_backup_049`;
    yield* sql`
    INSERT INTO agent_control_worktree_event_envelopes (
      event_id, reservation_id, stream_version, event_type, project_id, task_id,
      stage_run_id, attempt_id, lease_id, fence_token, created_at
    )
    SELECT event_id, reservation_id, stream_version, event_type, project_id, task_id,
      stage_run_id, attempt_id, lease_id, fence_token, created_at
    FROM agent_control_worktree_event_envelopes_backup_049
  `;
    yield* sql`DROP TABLE agent_control_worktree_event_envelopes_backup_049`;
    for (const trigger of dependentTriggers) {
      if (trigger.sql !== null && !controlledThreadTriggerNames.has(trigger.name)) {
        yield* sql.unsafe(trigger.sql).unprepared;
      }
    }
    yield* sql`
    CREATE TABLE agent_control_controlled_thread_json_validation_049 (
      valid INTEGER NOT NULL CHECK (valid = 1)
    )
  `;
    yield* sql.unsafe(
      `INSERT INTO agent_control_controlled_thread_json_validation_049(valid)
       SELECT COALESCE((${reservationPayloadJsonTotalPredicate.replaceAll("NEW.", "event.")}), 0)
       FROM agent_control_events event
       WHERE event.aggregate_kind = 'controlled-thread-reservation'`,
    ).unprepared;
    yield* sql.unsafe(
      `INSERT INTO agent_control_controlled_thread_json_validation_049(valid)
       SELECT COALESCE((${reservationProjectionJsonTotalPredicate.replaceAll("NEW.", "state.")}), 0)
       FROM agent_control_controlled_thread_reservation_states state`,
    ).unprepared;
    yield* sql`DROP TABLE agent_control_controlled_thread_json_validation_049`;
    yield* sql`
    CREATE INDEX idx_agent_control_controlled_thread_catalog_position
    ON agent_control_controlled_thread_stream_catalog(
      project_id, task_id, stage_run_id, attempt_id,
      task_revision, github_intake_sequence, source_identity_fingerprint
    )
  `;
    yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_controlled_thread_catalog_coordinator
    ON agent_control_controlled_thread_stream_catalog(
      coordinator_command_id, coordinator_command_fingerprint,
      materialization_command_id, materialization_command_fingerprint,
      stream_version
    )
    WHERE coordinator_command_id IS NOT NULL
  `;

    yield* sql`
    CREATE TABLE agent_control_controlled_thread_materialization_intents (
      coordinator_command_id TEXT PRIMARY KEY,
      request_fingerprint TEXT NOT NULL CHECK (
        length(request_fingerprint) = 64
        AND request_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      coordinator_command_fingerprint TEXT NOT NULL CHECK (
        length(coordinator_command_fingerprint) = 64
        AND coordinator_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      policy_binding_fingerprint TEXT NOT NULL CHECK (
        length(policy_binding_fingerprint) = 64
        AND policy_binding_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      runtime_observation_fingerprint TEXT NOT NULL CHECK (
        length(runtime_observation_fingerprint) = 64
        AND runtime_observation_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      project_id TEXT NOT NULL,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        length(source_identity_fingerprint) = 64
        AND source_identity_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      stage_run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      role_id TEXT NOT NULL CHECK (role_id = 'planning'),
      stage_kind TEXT NOT NULL CHECK (stage_kind = 'planning'),
      stage_ordinal INTEGER NOT NULL CHECK (stage_ordinal = 1),
      attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal = 1),
      lease_id TEXT NOT NULL,
      lease_holder_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 1),
      worktree_reservation_id TEXT NOT NULL,
      materializing_transition_command_id TEXT NOT NULL UNIQUE,
      bound_transition_command_id TEXT NOT NULL UNIQUE,
      materialization_command_id TEXT NOT NULL UNIQUE,
      materialization_command_fingerprint TEXT NOT NULL CHECK (
        length(materialization_command_fingerprint) = 64
        AND materialization_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      title TEXT NOT NULL CHECK (length(trim(title)) > 0),
      model_selection_json TEXT NOT NULL CHECK (
        COALESCE(
          json_valid(model_selection_json) = 1
          AND json_type(model_selection_json) = 'object',
          0
        ) = 1
      ),
      runtime_mode TEXT NOT NULL CHECK (
        runtime_mode IN ('approval-required', 'full-access')
      ),
      interaction_mode TEXT NOT NULL CHECK (interaction_mode = 'plan'),
      branch TEXT NOT NULL CHECK (length(trim(branch)) > 0),
      worktree_path TEXT NOT NULL CHECK (length(trim(worktree_path)) > 0),
      binding_json TEXT NOT NULL CHECK (
        COALESCE(
          json_valid(binding_json) = 1
          AND json_type(binding_json) = 'object'
          AND json_extract(binding_json, '$.taskId') = task_id
          AND json_extract(binding_json, '$.stageRunId') = stage_run_id
          AND json_extract(binding_json, '$.attemptId') = attempt_id
          AND json_extract(binding_json, '$.roleId') = role_id
          AND json_extract(binding_json, '$.controlState') = 'controlled',
          0
        ) = 1
      ),
      materializing_event_id TEXT NOT NULL UNIQUE,
      materializing_event_sequence INTEGER NOT NULL CHECK (materializing_event_sequence >= 1),
      bound_event_id TEXT NOT NULL UNIQUE,
      bound_event_sequence INTEGER NOT NULL CHECK (
        bound_event_sequence > materializing_event_sequence
      ),
      orchestration_result_sequence INTEGER NOT NULL CHECK (
        orchestration_result_sequence >= 1
      ),
      materializing_at TEXT NOT NULL,
      materialized_at TEXT NOT NULL,
      bound_at TEXT NOT NULL,
      accepted_at TEXT NOT NULL,
      accepted_marker_command_id TEXT NOT NULL,
      CHECK (
        accepted_marker_command_id = coordinator_command_id
        AND materializing_at <= materialized_at
        AND materialized_at <= bound_at
        AND bound_at <= accepted_at
      ),
      UNIQUE (
        coordinator_command_id, coordinator_command_fingerprint,
        materialization_command_id, materialization_command_fingerprint
      ),
      UNIQUE (
        coordinator_command_id, coordinator_command_fingerprint,
        controlled_thread_reservation_id, thread_id,
        materialization_command_id, materialization_command_fingerprint,
        orchestration_result_sequence, accepted_at
      ),
      FOREIGN KEY (accepted_marker_command_id)
      REFERENCES agent_control_controlled_thread_materialization_accepted(
        coordinator_command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

    yield* sql`
    CREATE TABLE agent_control_controlled_thread_materialization_receipts (
      coordinator_command_id TEXT PRIMARY KEY,
      request_fingerprint TEXT NOT NULL CHECK (
        length(request_fingerprint) = 64
        AND request_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      coordinator_command_fingerprint TEXT NOT NULL CHECK (
        length(coordinator_command_fingerprint) = 64
        AND coordinator_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      materialization_command_id TEXT NOT NULL UNIQUE,
      materialization_command_fingerprint TEXT NOT NULL CHECK (
        length(materialization_command_fingerprint) = 64
        AND materialization_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      orchestration_result_sequence INTEGER NOT NULL CHECK (
        orchestration_result_sequence >= 1
      ),
      status TEXT NOT NULL CHECK (status = 'accepted'),
      accepted_at TEXT NOT NULL,
      accepted_marker_command_id TEXT NOT NULL,
      CHECK (accepted_marker_command_id = coordinator_command_id),
      UNIQUE (
        coordinator_command_id, coordinator_command_fingerprint,
        controlled_thread_reservation_id, thread_id,
        materialization_command_id, materialization_command_fingerprint,
        orchestration_result_sequence, accepted_at
      ),
      FOREIGN KEY (
        coordinator_command_id, coordinator_command_fingerprint,
        controlled_thread_reservation_id, thread_id,
        materialization_command_id, materialization_command_fingerprint,
        orchestration_result_sequence, accepted_at
      ) REFERENCES agent_control_controlled_thread_materialization_intents(
        coordinator_command_id, coordinator_command_fingerprint,
        controlled_thread_reservation_id, thread_id,
        materialization_command_id, materialization_command_fingerprint,
        orchestration_result_sequence, accepted_at
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (accepted_marker_command_id)
      REFERENCES agent_control_controlled_thread_materialization_accepted(
        coordinator_command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

    yield* sql`
    CREATE TABLE agent_control_controlled_thread_materialization_accepted (
      coordinator_command_id TEXT PRIMARY KEY,
      coordinator_command_fingerprint TEXT NOT NULL CHECK (
        length(coordinator_command_fingerprint) = 64
        AND coordinator_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      materialization_command_id TEXT NOT NULL UNIQUE,
      materialization_command_fingerprint TEXT NOT NULL CHECK (
        length(materialization_command_fingerprint) = 64
        AND materialization_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      orchestration_result_sequence INTEGER NOT NULL CHECK (
        orchestration_result_sequence >= 1
      ),
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (
        coordinator_command_id, coordinator_command_fingerprint,
        controlled_thread_reservation_id, thread_id,
        materialization_command_id, materialization_command_fingerprint,
        orchestration_result_sequence, accepted_at
      ) REFERENCES agent_control_controlled_thread_materialization_receipts(
        coordinator_command_id, coordinator_command_fingerprint,
        controlled_thread_reservation_id, thread_id,
        materialization_command_id, materialization_command_fingerprint,
        orchestration_result_sequence, accepted_at
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (
        materialization_command_id, materialization_command_fingerprint,
        thread_id, orchestration_result_sequence, accepted_at
      ) REFERENCES orchestration_agent_control_thread_materialization_receipts(
        command_id, command_fingerprint, thread_id, result_sequence, accepted_at
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;
    yield* sql`
    CREATE UNIQUE INDEX idx_orchestration_materialization_coordinator_parent
    ON orchestration_agent_control_thread_materialization_receipts(
      command_id, command_fingerprint, thread_id, result_sequence, accepted_at
    )
  `;
    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_coordinator_intent_json_validate
    BEFORE INSERT ON agent_control_controlled_thread_materialization_intents
    WHEN NOT COALESCE((
      json_valid(NEW.model_selection_json) = 1
      AND json_type(NEW.model_selection_json) = 'object'
      AND (SELECT count(*) FROM json_each(NEW.model_selection_json)) IN (2, 3)
      AND (SELECT count(*) FROM json_each(NEW.model_selection_json)) =
        (SELECT count(DISTINCT key) FROM json_each(NEW.model_selection_json))
      AND NOT EXISTS (
        SELECT 1 FROM json_each(NEW.model_selection_json)
        WHERE key NOT IN ('instanceId', 'model', 'options')
      )
      AND (
        SELECT count(*) FROM json_each(NEW.model_selection_json)
        WHERE key = 'instanceId'
      ) = 1
      AND json_type(NEW.model_selection_json, '$.instanceId') = 'text'
      AND length(trim(json_extract(NEW.model_selection_json, '$.instanceId'))) > 0
      AND (
        SELECT count(*) FROM json_each(NEW.model_selection_json)
        WHERE key = 'model'
      ) = 1
      AND json_type(NEW.model_selection_json, '$.model') = 'text'
      AND length(trim(json_extract(NEW.model_selection_json, '$.model'))) > 0
      AND (
        (
          SELECT count(*) FROM json_each(NEW.model_selection_json)
          WHERE key = 'options'
        ) = 0
        OR (
          (
            SELECT count(*) FROM json_each(NEW.model_selection_json)
            WHERE key = 'options'
          ) = 1
          AND json_type(NEW.model_selection_json, '$.options') = 'array'
          AND NOT EXISTS (
            SELECT 1
            FROM json_each(NEW.model_selection_json, '$.options') option
            WHERE json_type(option.value) <> 'object'
              OR (SELECT count(*) FROM json_each(option.value)) <> 2
              OR (SELECT count(DISTINCT key) FROM json_each(option.value)) <> 2
              OR EXISTS (
                SELECT 1 FROM json_each(option.value)
                WHERE key NOT IN ('id', 'value')
              )
              OR (
                SELECT count(*) FROM json_each(option.value)
                WHERE key = 'id'
              ) <> 1
              OR json_type(option.value, '$.id') <> 'text'
              OR length(trim(json_extract(option.value, '$.id'))) = 0
              OR (
                SELECT count(*) FROM json_each(option.value)
                WHERE key = 'value'
              ) <> 1
              OR json_type(option.value, '$.value') NOT IN ('text', 'true', 'false')
              OR (
                json_type(option.value, '$.value') = 'text'
                AND length(trim(json_extract(option.value, '$.value'))) = 0
              )
          )
          AND NOT EXISTS (
            SELECT 1
            FROM json_each(NEW.model_selection_json, '$.options') option
            GROUP BY json_extract(option.value, '$.id')
            HAVING count(*) <> 1
          )
        )
      )
      AND json_valid(NEW.binding_json) = 1
      AND json_type(NEW.binding_json) = 'object'
      AND (SELECT count(*) FROM json_each(NEW.binding_json)) = 5
      AND (SELECT count(DISTINCT key) FROM json_each(NEW.binding_json)) = 5
      AND NOT EXISTS (
        SELECT 1 FROM json_each(NEW.binding_json)
        WHERE key NOT IN ('taskId', 'stageRunId', 'attemptId', 'roleId', 'controlState')
      )
      AND (
        SELECT count(*) FROM json_each(NEW.binding_json)
        WHERE key = 'taskId'
      ) = 1
      AND json_type(NEW.binding_json, '$.taskId') = 'text'
      AND json_extract(NEW.binding_json, '$.taskId') = NEW.task_id
      AND (
        SELECT count(*) FROM json_each(NEW.binding_json)
        WHERE key = 'stageRunId'
      ) = 1
      AND json_type(NEW.binding_json, '$.stageRunId') = 'text'
      AND json_extract(NEW.binding_json, '$.stageRunId') = NEW.stage_run_id
      AND (
        SELECT count(*) FROM json_each(NEW.binding_json)
        WHERE key = 'attemptId'
      ) = 1
      AND json_type(NEW.binding_json, '$.attemptId') = 'text'
      AND json_extract(NEW.binding_json, '$.attemptId') = NEW.attempt_id
      AND (
        SELECT count(*) FROM json_each(NEW.binding_json)
        WHERE key = 'roleId'
      ) = 1
      AND json_type(NEW.binding_json, '$.roleId') = 'text'
      AND json_extract(NEW.binding_json, '$.roleId') = NEW.role_id
      AND (
        SELECT count(*) FROM json_each(NEW.binding_json)
        WHERE key = 'controlState'
      ) = 1
      AND json_type(NEW.binding_json, '$.controlState') = 'text'
      AND json_extract(NEW.binding_json, '$.controlState') = 'controlled'
    ), 0)
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread coordinator intent json is noncanonical');
    END
  `;

    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_catalog_stable_validate
    BEFORE INSERT ON agent_control_controlled_thread_stream_catalog
    WHEN NEW.stream_version > 1
    BEGIN
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM agent_control_controlled_thread_stream_catalog prepared
        WHERE prepared.controlled_thread_reservation_id =
          NEW.controlled_thread_reservation_id
          AND prepared.stream_version = 1
          AND prepared.thread_id IS NEW.thread_id
          AND prepared.project_id IS NEW.project_id
          AND prepared.task_id IS NEW.task_id
          AND prepared.task_revision IS NEW.task_revision
          AND prepared.github_intake_sequence IS NEW.github_intake_sequence
          AND prepared.source_identity_fingerprint IS NEW.source_identity_fingerprint
          AND prepared.stage_run_id IS NEW.stage_run_id
          AND prepared.attempt_id IS NEW.attempt_id
          AND prepared.role_id IS NEW.role_id
          AND prepared.stage_kind IS NEW.stage_kind
          AND prepared.stage_ordinal IS NEW.stage_ordinal
          AND prepared.attempt_ordinal IS NEW.attempt_ordinal
          AND prepared.lease_id IS NEW.lease_id
          AND prepared.fence_token IS NEW.fence_token
          AND prepared.worktree_reservation_id IS NEW.worktree_reservation_id
          AND prepared.prepared_at IS NEW.prepared_at
      ) THEN RAISE(ABORT, 'controlled thread reservation stable binding changed') END;
      SELECT CASE WHEN NEW.stream_version = 3 AND NOT EXISTS (
        SELECT 1
        FROM agent_control_controlled_thread_stream_catalog materializing
        WHERE materializing.controlled_thread_reservation_id =
          NEW.controlled_thread_reservation_id
          AND materializing.stream_version = 2
          AND materializing.coordinator_command_id IS NEW.coordinator_command_id
          AND materializing.coordinator_command_fingerprint IS
            NEW.coordinator_command_fingerprint
          AND materializing.materializing_transition_command_id IS
            NEW.materializing_transition_command_id
          AND materializing.materialization_command_id IS NEW.materialization_command_id
          AND materializing.materialization_command_fingerprint IS
            NEW.materialization_command_fingerprint
          AND materializing.lease_holder_id IS NEW.lease_holder_id
          AND materializing.materializing_at IS NEW.materializing_at
      ) THEN RAISE(ABORT, 'controlled thread materializing binding changed') END;
    END
  `;
    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_catalog_no_update
    BEFORE UPDATE ON agent_control_controlled_thread_stream_catalog
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread stream catalog is immutable');
    END
  `;
    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_catalog_no_delete
    BEFORE DELETE ON agent_control_controlled_thread_stream_catalog
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread stream catalog is immutable');
    END
  `;

    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'controlled-thread-reservation'
    BEGIN
      SELECT CASE WHEN COALESCE((
        NEW.actor_authority = 'controller'
        AND NEW.causation_event_id IS NULL
        AND json_valid(NEW.payload_json) = 1
        AND json_type(NEW.payload_json) = 'object'
        AND json_valid(NEW.metadata_json) = 1
        AND json_type(NEW.metadata_json) = 'object'
        AND (SELECT count(*) FROM json_each(NEW.metadata_json)) = 1
        AND json_extract(NEW.metadata_json, '$.schemaVersion') = 1
        AND NEW.stream_id =
          json_extract(NEW.payload_json, '$.controlledThreadReservationId')
        AND json_extract(NEW.payload_json, '$.threadId') IS NOT NULL
        AND json_extract(NEW.payload_json, '$.projectId') IS NOT NULL
        AND json_extract(NEW.payload_json, '$.taskId') IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM agent_control_controlled_thread_stream_catalog catalog
          WHERE catalog.controlled_thread_reservation_id = NEW.stream_id
            AND catalog.event_id = NEW.event_id
            AND catalog.stream_version = NEW.stream_version
            AND catalog.command_id = NEW.command_id
            AND catalog.event_type = NEW.event_type
            AND catalog.thread_id = json_extract(NEW.payload_json, '$.threadId')
            AND catalog.project_id = json_extract(NEW.payload_json, '$.projectId')
            AND catalog.task_id = json_extract(NEW.payload_json, '$.taskId')
            AND catalog.task_revision =
              json_extract(NEW.payload_json, '$.taskRevision')
            AND catalog.github_intake_sequence =
              json_extract(NEW.payload_json, '$.githubIntakeSequence')
            AND catalog.source_identity_fingerprint =
              json_extract(NEW.payload_json, '$.sourceIdentityFingerprint')
            AND catalog.stage_run_id = json_extract(NEW.payload_json, '$.stageRunId')
            AND catalog.attempt_id = json_extract(NEW.payload_json, '$.attemptId')
            AND catalog.role_id = json_extract(NEW.payload_json, '$.roleId')
            AND catalog.stage_kind = json_extract(NEW.payload_json, '$.stageKind')
            AND catalog.stage_ordinal = json_extract(NEW.payload_json, '$.stageOrdinal')
            AND catalog.attempt_ordinal =
              json_extract(NEW.payload_json, '$.attemptOrdinal')
            AND catalog.lease_id = json_extract(NEW.payload_json, '$.leaseId')
            AND catalog.fence_token = json_extract(NEW.payload_json, '$.fenceToken')
            AND catalog.worktree_reservation_id =
              json_extract(NEW.payload_json, '$.worktreeReservationId')
            AND catalog.prepared_at = json_extract(NEW.payload_json, '$.preparedAt')
            AND catalog.coordinator_command_id IS
              json_extract(NEW.payload_json, '$.coordinatorCommandId')
            AND catalog.coordinator_command_fingerprint IS
              json_extract(NEW.payload_json, '$.coordinatorCommandFingerprint')
            AND catalog.materializing_transition_command_id IS
              json_extract(NEW.payload_json, '$.materializingTransitionCommandId')
            AND catalog.materialization_command_id IS
              json_extract(NEW.payload_json, '$.materializationCommandId')
            AND catalog.materialization_command_fingerprint IS
              json_extract(NEW.payload_json, '$.materializationCommandFingerprint')
            AND catalog.lease_holder_id IS
              json_extract(NEW.payload_json, '$.leaseHolderId')
            AND catalog.materializing_at IS
              json_extract(NEW.payload_json, '$.materializingAt')
            AND catalog.bound_transition_command_id IS
              json_extract(NEW.payload_json, '$.boundTransitionCommandId')
            AND catalog.orchestration_result_sequence IS
              json_extract(NEW.payload_json, '$.orchestrationResultSequence')
            AND catalog.materialized_at IS
              json_extract(NEW.payload_json, '$.materializedAt')
            AND catalog.bound_at IS json_extract(NEW.payload_json, '$.boundAt')
        )
        AND (
          (
            NEW.stream_version = 1
            AND NEW.event_type = 'agentControl.controlledThreadReservation.prepared'
            AND NEW.correlation_id = NEW.command_id
            AND json_extract(NEW.payload_json, '$.status') = 'prepared'
            AND NEW.occurred_at = json_extract(NEW.payload_json, '$.preparedAt')
          )
          OR (
            NEW.stream_version = 2
            AND NEW.event_type = 'agentControl.controlledThreadReservation.materializing'
            AND NEW.correlation_id =
              json_extract(NEW.payload_json, '$.coordinatorCommandId')
            AND NEW.command_id =
              json_extract(NEW.payload_json, '$.materializingTransitionCommandId')
            AND json_extract(NEW.payload_json, '$.status') = 'materializing'
            AND NEW.occurred_at = json_extract(NEW.payload_json, '$.materializingAt')
          )
          OR (
            NEW.stream_version = 3
            AND NEW.event_type = 'agentControl.controlledThreadReservation.bound'
            AND NEW.correlation_id =
              json_extract(NEW.payload_json, '$.coordinatorCommandId')
            AND NEW.command_id =
              json_extract(NEW.payload_json, '$.boundTransitionCommandId')
            AND json_extract(NEW.payload_json, '$.status') = 'bound'
            AND NEW.occurred_at = json_extract(NEW.payload_json, '$.boundAt')
          )
        )
      ), 0) <> 1
      THEN RAISE(ABORT, 'invalid controlled thread reservation event') END;
    END
  `;
    yield* sql.unsafe(
      `CREATE TRIGGER agent_control_controlled_thread_event_json_total_validate
       BEFORE INSERT ON agent_control_events
       WHEN NEW.aggregate_kind = 'controlled-thread-reservation'
         AND NOT COALESCE((${reservationPayloadJsonTotalPredicate}), 0)
       BEGIN
         SELECT RAISE(ABORT, 'controlled thread reservation event json is noncanonical');
       END`,
    ).unprepared;
    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_event_no_update
    BEFORE UPDATE ON agent_control_events
    WHEN OLD.aggregate_kind = 'controlled-thread-reservation'
      OR NEW.aggregate_kind = 'controlled-thread-reservation'
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread reservation events are immutable');
    END
  `;
    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_event_no_delete
    BEFORE DELETE ON agent_control_events
    WHEN OLD.aggregate_kind = 'controlled-thread-reservation'
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread reservation events are immutable');
    END
  `;

    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_projection_validate_insert
    BEFORE INSERT ON agent_control_controlled_thread_reservation_states
    BEGIN
      SELECT CASE WHEN COALESCE((
        json_valid(NEW.state_json) = 1
        AND json_type(NEW.state_json) = 'object'
        AND NEW.controlled_thread_reservation_id =
          json_extract(NEW.state_json, '$.controlledThreadReservationId')
        AND NEW.thread_id = json_extract(NEW.state_json, '$.threadId')
        AND NEW.project_id = json_extract(NEW.state_json, '$.projectId')
        AND NEW.task_id = json_extract(NEW.state_json, '$.taskId')
        AND NEW.task_revision = json_extract(NEW.state_json, '$.taskRevision')
        AND NEW.github_intake_sequence =
          json_extract(NEW.state_json, '$.githubIntakeSequence')
        AND NEW.source_identity_fingerprint =
          json_extract(NEW.state_json, '$.sourceIdentityFingerprint')
        AND NEW.stage_run_id = json_extract(NEW.state_json, '$.stageRunId')
        AND NEW.attempt_id = json_extract(NEW.state_json, '$.attemptId')
        AND NEW.role_id = json_extract(NEW.state_json, '$.roleId')
        AND NEW.stage_kind = json_extract(NEW.state_json, '$.stageKind')
        AND NEW.stage_ordinal = json_extract(NEW.state_json, '$.stageOrdinal')
        AND NEW.attempt_ordinal = json_extract(NEW.state_json, '$.attemptOrdinal')
        AND NEW.lease_id = json_extract(NEW.state_json, '$.leaseId')
        AND NEW.fence_token = json_extract(NEW.state_json, '$.fenceToken')
        AND NEW.worktree_reservation_id =
          json_extract(NEW.state_json, '$.worktreeReservationId')
        AND NEW.status = json_extract(NEW.state_json, '$.status')
        AND NEW.revision = json_extract(NEW.state_json, '$.revision')
        AND NEW.last_event_sequence = json_extract(NEW.state_json, '$.sequence')
        AND NEW.prepared_at = json_extract(NEW.state_json, '$.preparedAt')
        AND NEW.coordinator_command_id IS
          json_extract(NEW.state_json, '$.coordinatorCommandId')
        AND NEW.coordinator_command_fingerprint IS
          json_extract(NEW.state_json, '$.coordinatorCommandFingerprint')
        AND NEW.materializing_transition_command_id IS
          json_extract(NEW.state_json, '$.materializingTransitionCommandId')
        AND NEW.materialization_command_id IS
          json_extract(NEW.state_json, '$.materializationCommandId')
        AND NEW.materialization_command_fingerprint IS
          json_extract(NEW.state_json, '$.materializationCommandFingerprint')
        AND NEW.lease_holder_id IS json_extract(NEW.state_json, '$.leaseHolderId')
        AND NEW.materializing_at IS json_extract(NEW.state_json, '$.materializingAt')
        AND NEW.bound_transition_command_id IS
          json_extract(NEW.state_json, '$.boundTransitionCommandId')
        AND NEW.orchestration_result_sequence IS
          json_extract(NEW.state_json, '$.orchestrationResultSequence')
        AND NEW.materialized_at IS json_extract(NEW.state_json, '$.materializedAt')
        AND NEW.bound_at IS json_extract(NEW.state_json, '$.boundAt')
        AND EXISTS (
          SELECT 1
          FROM agent_control_controlled_thread_stream_catalog catalog
          JOIN agent_control_events event ON event.event_id = catalog.event_id
          WHERE catalog.controlled_thread_reservation_id =
            NEW.controlled_thread_reservation_id
            AND catalog.stream_version = NEW.revision
            AND event.sequence = NEW.last_event_sequence
        )
      ), 0) <> 1
      THEN RAISE(ABORT, 'invalid controlled thread reservation projection') END;
    END
  `;
    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_projection_validate_update
    BEFORE UPDATE ON agent_control_controlled_thread_reservation_states
    BEGIN
      SELECT CASE WHEN NOT (
        OLD.controlled_thread_reservation_id IS NEW.controlled_thread_reservation_id
        AND OLD.thread_id IS NEW.thread_id
        AND OLD.project_id IS NEW.project_id
        AND OLD.task_id IS NEW.task_id
        AND OLD.task_revision IS NEW.task_revision
        AND OLD.github_intake_sequence IS NEW.github_intake_sequence
        AND OLD.source_identity_fingerprint IS NEW.source_identity_fingerprint
        AND OLD.stage_run_id IS NEW.stage_run_id
        AND OLD.attempt_id IS NEW.attempt_id
        AND OLD.role_id IS NEW.role_id
        AND OLD.stage_kind IS NEW.stage_kind
        AND OLD.stage_ordinal IS NEW.stage_ordinal
        AND OLD.attempt_ordinal IS NEW.attempt_ordinal
        AND OLD.lease_id IS NEW.lease_id
        AND OLD.fence_token IS NEW.fence_token
        AND OLD.worktree_reservation_id IS NEW.worktree_reservation_id
        AND OLD.prepared_at IS NEW.prepared_at
        AND NEW.revision = OLD.revision + 1
        AND NEW.last_event_sequence > OLD.last_event_sequence
        AND ((OLD.status = 'prepared' AND NEW.status = 'materializing')
          OR (OLD.status = 'materializing' AND NEW.status = 'bound'))
      ) THEN RAISE(ABORT, 'invalid controlled thread reservation transition') END;
    END
  `;
    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_projection_validate_update_json
    AFTER UPDATE ON agent_control_controlled_thread_reservation_states
    WHEN NOT COALESCE((
      json_valid(NEW.state_json) = 1
      AND NEW.controlled_thread_reservation_id =
        json_extract(NEW.state_json, '$.controlledThreadReservationId')
      AND NEW.status = json_extract(NEW.state_json, '$.status')
      AND NEW.revision = json_extract(NEW.state_json, '$.revision')
      AND NEW.last_event_sequence = json_extract(NEW.state_json, '$.sequence')
      AND NEW.coordinator_command_id IS
        json_extract(NEW.state_json, '$.coordinatorCommandId')
      AND NEW.coordinator_command_fingerprint IS
        json_extract(NEW.state_json, '$.coordinatorCommandFingerprint')
      AND NEW.materializing_transition_command_id IS
        json_extract(NEW.state_json, '$.materializingTransitionCommandId')
      AND NEW.materialization_command_id IS
        json_extract(NEW.state_json, '$.materializationCommandId')
      AND NEW.materialization_command_fingerprint IS
        json_extract(NEW.state_json, '$.materializationCommandFingerprint')
      AND NEW.lease_holder_id IS json_extract(NEW.state_json, '$.leaseHolderId')
      AND NEW.materializing_at IS json_extract(NEW.state_json, '$.materializingAt')
      AND NEW.bound_transition_command_id IS
        json_extract(NEW.state_json, '$.boundTransitionCommandId')
      AND NEW.orchestration_result_sequence IS
        json_extract(NEW.state_json, '$.orchestrationResultSequence')
      AND NEW.materialized_at IS json_extract(NEW.state_json, '$.materializedAt')
      AND NEW.bound_at IS json_extract(NEW.state_json, '$.boundAt')
      AND EXISTS (
        SELECT 1
        FROM agent_control_controlled_thread_stream_catalog catalog
        JOIN agent_control_events event ON event.event_id = catalog.event_id
        WHERE catalog.controlled_thread_reservation_id =
          NEW.controlled_thread_reservation_id
          AND catalog.stream_version = NEW.revision
          AND event.sequence = NEW.last_event_sequence
      )
    ), 0)
    BEGIN
      SELECT RAISE(ABORT, 'invalid controlled thread reservation projection');
    END
  `;
    for (const operation of ["INSERT", "UPDATE"] as const) {
      yield* sql.unsafe(
        `CREATE TRIGGER agent_control_controlled_thread_projection_json_total_validate_${operation.toLowerCase()}
         BEFORE ${operation} ON agent_control_controlled_thread_reservation_states
         WHEN NOT COALESCE((${reservationProjectionJsonTotalPredicate}), 0)
         BEGIN
           SELECT RAISE(
             ABORT,
             'controlled thread reservation projection json is noncanonical'
           );
         END`,
      ).unprepared;
    }

    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_coordinator_receipt_validate
    BEFORE INSERT ON agent_control_controlled_thread_materialization_receipts
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_controlled_thread_materialization_intents intent
      WHERE intent.coordinator_command_id IS NEW.coordinator_command_id
        AND intent.request_fingerprint IS NEW.request_fingerprint
        AND intent.coordinator_command_fingerprint IS
          NEW.coordinator_command_fingerprint
        AND intent.controlled_thread_reservation_id IS
          NEW.controlled_thread_reservation_id
        AND intent.thread_id IS NEW.thread_id
        AND intent.materialization_command_id IS NEW.materialization_command_id
        AND intent.materialization_command_fingerprint IS
          NEW.materialization_command_fingerprint
        AND intent.orchestration_result_sequence IS NEW.orchestration_result_sequence
        AND intent.accepted_at IS NEW.accepted_at
        AND NEW.status IS 'accepted'
        AND NEW.accepted_marker_command_id IS NEW.coordinator_command_id
    )
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread coordinator receipt is inconsistent');
    END
  `;

    yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_coordinator_accepted_validate
    BEFORE INSERT ON agent_control_controlled_thread_materialization_accepted
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_controlled_thread_materialization_intents intent
      JOIN agent_control_controlled_thread_materialization_receipts receipt
        ON receipt.coordinator_command_id IS intent.coordinator_command_id
       AND receipt.request_fingerprint IS intent.request_fingerprint
       AND receipt.coordinator_command_fingerprint IS
         intent.coordinator_command_fingerprint
       AND receipt.controlled_thread_reservation_id IS
         intent.controlled_thread_reservation_id
       AND receipt.thread_id IS intent.thread_id
       AND receipt.materialization_command_id IS intent.materialization_command_id
       AND receipt.materialization_command_fingerprint IS
         intent.materialization_command_fingerprint
       AND receipt.orchestration_result_sequence IS intent.orchestration_result_sequence
       AND receipt.accepted_at IS intent.accepted_at
       AND receipt.status IS 'accepted'
      JOIN agent_control_controlled_thread_stream_catalog materializing
        ON materializing.controlled_thread_reservation_id IS
          intent.controlled_thread_reservation_id
       AND materializing.stream_version IS 2
       AND materializing.event_id IS intent.materializing_event_id
       AND materializing.command_id IS intent.materializing_transition_command_id
       AND materializing.coordinator_command_id IS intent.coordinator_command_id
       AND materializing.coordinator_command_fingerprint IS
         intent.coordinator_command_fingerprint
       AND materializing.materialization_command_id IS
         intent.materialization_command_id
       AND materializing.materialization_command_fingerprint IS
         intent.materialization_command_fingerprint
      JOIN agent_control_events materializing_event
        ON materializing_event.event_id IS materializing.event_id
       AND materializing_event.sequence IS intent.materializing_event_sequence
      JOIN agent_control_controlled_thread_stream_catalog bound
        ON bound.controlled_thread_reservation_id IS
          intent.controlled_thread_reservation_id
       AND bound.stream_version IS 3
       AND bound.event_id IS intent.bound_event_id
       AND bound.command_id IS intent.bound_transition_command_id
       AND bound.coordinator_command_id IS intent.coordinator_command_id
       AND bound.coordinator_command_fingerprint IS
         intent.coordinator_command_fingerprint
       AND bound.materialization_command_id IS intent.materialization_command_id
       AND bound.materialization_command_fingerprint IS
         intent.materialization_command_fingerprint
       AND bound.orchestration_result_sequence IS
         intent.orchestration_result_sequence
      JOIN agent_control_events bound_event
        ON bound_event.event_id IS bound.event_id
       AND bound_event.sequence IS intent.bound_event_sequence
      JOIN agent_control_controlled_thread_reservation_states reservation
        ON reservation.controlled_thread_reservation_id IS
          intent.controlled_thread_reservation_id
       AND reservation.thread_id IS intent.thread_id
       AND reservation.status IS 'bound'
       AND reservation.revision IS 3
       AND reservation.last_event_sequence IS intent.bound_event_sequence
       AND reservation.coordinator_command_id IS intent.coordinator_command_id
       AND reservation.coordinator_command_fingerprint IS
         intent.coordinator_command_fingerprint
       AND reservation.materializing_transition_command_id IS
         intent.materializing_transition_command_id
       AND reservation.bound_transition_command_id IS
         intent.bound_transition_command_id
       AND reservation.materialization_command_id IS
         intent.materialization_command_id
       AND reservation.materialization_command_fingerprint IS
         intent.materialization_command_fingerprint
       AND reservation.orchestration_result_sequence IS
         intent.orchestration_result_sequence
       AND reservation.task_id IS intent.task_id
       AND reservation.task_revision IS intent.task_revision
       AND reservation.github_intake_sequence IS intent.github_intake_sequence
       AND reservation.source_identity_fingerprint IS
         intent.source_identity_fingerprint
       AND reservation.stage_run_id IS intent.stage_run_id
       AND reservation.attempt_id IS intent.attempt_id
       AND reservation.role_id IS intent.role_id
       AND reservation.stage_kind IS intent.stage_kind
       AND reservation.stage_ordinal IS intent.stage_ordinal
       AND reservation.attempt_ordinal IS intent.attempt_ordinal
       AND reservation.lease_id IS intent.lease_id
       AND reservation.fence_token IS intent.fence_token
       AND reservation.worktree_reservation_id IS intent.worktree_reservation_id
      JOIN agent_control_task_states task
        ON task.task_id IS intent.task_id
       AND task.project_id IS intent.project_id
       AND task.revision IS intent.task_revision
       AND task.github_intake_sequence IS intent.github_intake_sequence
       AND task.status IS 'candidate'
       AND task.source_gate IS 'eligible'
       AND task.stage IS 'intake'
      JOIN agent_control_stage_run_states stage
        ON stage.stage_run_id IS intent.stage_run_id
       AND stage.project_id IS intent.project_id
       AND stage.task_id IS intent.task_id
       AND stage.attempt_id IS intent.attempt_id
       AND stage.role_id IS intent.role_id
       AND stage.stage_kind IS intent.stage_kind
       AND stage.stage_ordinal IS intent.stage_ordinal
       AND stage.attempt_ordinal IS intent.attempt_ordinal
       AND stage.task_revision IS intent.task_revision
       AND stage.github_intake_sequence IS intent.github_intake_sequence
       AND stage.source_identity_fingerprint IS intent.source_identity_fingerprint
       AND stage.status IS 'prepared'
      JOIN agent_control_stage_run_lease_states lease
        ON lease.lease_id IS intent.lease_id
       AND lease.project_id IS intent.project_id
       AND lease.task_id IS intent.task_id
       AND lease.stage_run_id IS intent.stage_run_id
       AND lease.attempt_id IS intent.attempt_id
       AND lease.task_revision IS intent.task_revision
       AND lease.github_intake_sequence IS intent.github_intake_sequence
       AND lease.source_identity_fingerprint IS intent.source_identity_fingerprint
       AND lease.holder_id IS intent.lease_holder_id
       AND lease.fence_token IS intent.fence_token
       AND lease.status IS 'reserved'
      JOIN agent_control_worktree_reservation_states worktree
        ON worktree.reservation_id IS intent.worktree_reservation_id
       AND worktree.project_id IS intent.project_id
       AND worktree.task_id IS intent.task_id
       AND worktree.task_revision IS intent.task_revision
       AND worktree.github_intake_sequence IS intent.github_intake_sequence
       AND worktree.source_identity_fingerprint IS intent.source_identity_fingerprint
       AND worktree.stage_run_id IS intent.stage_run_id
       AND worktree.attempt_id IS intent.attempt_id
       AND worktree.lease_id IS intent.lease_id
       AND worktree.fence_token IS intent.fence_token
       AND worktree.branch_name IS intent.branch
       AND worktree.internal_worktree_path IS intent.worktree_path
       AND worktree.status IS 'ready'
      JOIN orchestration_agent_control_thread_materialization_receipts orchestration
        ON orchestration.command_id IS intent.materialization_command_id
       AND orchestration.command_fingerprint IS
         intent.materialization_command_fingerprint
       AND orchestration.thread_id IS intent.thread_id
       AND orchestration.result_sequence IS intent.orchestration_result_sequence
       AND orchestration.accepted_at IS intent.materialized_at
       AND orchestration.status IS 'accepted'
      JOIN projection_threads thread
        ON thread.thread_id IS intent.thread_id
       AND thread.project_id IS intent.project_id
       AND thread.title IS intent.title
       AND json(thread.model_selection_json) IS json(intent.model_selection_json)
       AND thread.runtime_mode IS intent.runtime_mode
       AND thread.interaction_mode IS intent.interaction_mode
       AND thread.branch IS intent.branch
       AND thread.worktree_path IS intent.worktree_path
       AND json(thread.agent_control_json) IS json(intent.binding_json)
      WHERE intent.coordinator_command_id IS NEW.coordinator_command_id
        AND intent.coordinator_command_fingerprint IS
          NEW.coordinator_command_fingerprint
        AND intent.controlled_thread_reservation_id IS
          NEW.controlled_thread_reservation_id
        AND intent.thread_id IS NEW.thread_id
        AND intent.materialization_command_id IS NEW.materialization_command_id
        AND intent.materialization_command_fingerprint IS
          NEW.materialization_command_fingerprint
        AND intent.orchestration_result_sequence IS NEW.orchestration_result_sequence
        AND intent.accepted_at IS NEW.accepted_at
        AND intent.accepted_marker_command_id IS NEW.coordinator_command_id
        AND receipt.accepted_marker_command_id IS NEW.coordinator_command_id
        AND materializing.lease_holder_id IS intent.lease_holder_id
        AND bound.lease_holder_id IS intent.lease_holder_id
        AND bound.materialized_at IS intent.materialized_at
        AND bound.bound_at IS intent.bound_at
        AND materializing_event.sequence < bound_event.sequence
        AND (
          SELECT count(*)
          FROM agent_control_events candidate
          WHERE candidate.aggregate_kind = 'controlled-thread-reservation'
            AND candidate.stream_id = intent.controlled_thread_reservation_id
        ) IS 3
        AND (
          SELECT count(*)
          FROM orchestration_events candidate
          WHERE candidate.command_id = intent.materialization_command_id
        ) IS 2
    )
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread coordinator accepted evidence is incomplete');
    END
  `;

    for (const [table, noun] of [
      [
        "agent_control_controlled_thread_materialization_intents",
        "controlled thread coordinator intent",
      ],
      [
        "agent_control_controlled_thread_materialization_receipts",
        "controlled thread coordinator receipt",
      ],
      [
        "agent_control_controlled_thread_materialization_accepted",
        "controlled thread coordinator accepted marker",
      ],
    ] as const) {
      const prefix = table;
      yield* sql.unsafe(
        `CREATE TRIGGER ${quoteSqliteIdentifier(`${prefix}_no_update`)}
         BEFORE UPDATE ON ${quoteSqliteIdentifier(table)}
         BEGIN SELECT RAISE(ABORT, '${noun} is immutable'); END`,
      ).unprepared;
      yield* sql.unsafe(
        `CREATE TRIGGER ${quoteSqliteIdentifier(`${prefix}_no_delete`)}
         BEFORE DELETE ON ${quoteSqliteIdentifier(table)}
         BEGIN SELECT RAISE(ABORT, '${noun} is immutable'); END`,
      ).unprepared;
    }
  }),
);
