import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const strictTextAllowEmpty = (column: string) => `
  typeof(${column}) = 'text'
  AND json_valid(json_array(${column})) = 1
  AND json_extract(json_array(${column}), '$[0]') IS ${column}
`;
const strictText = (column: string) =>
  `${strictTextAllowEmpty(column)} AND length(${column}) > 0 AND trim(${column}) = ${column}`;
const positive = (column: string) => `typeof(${column}) = 'integer' AND ${column} >= 1`;
const sha256 = (column: string) =>
  `${strictText(column)} AND length(${column}) = 64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;
const timestamp = (column: string) => `
  ${strictText(column)}
  AND length(${column}) = 24
  AND ${column} GLOB
    '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
  AND strftime('%Y-%m-%dT%H:%M:%fZ', ${column}) = ${column}
`;
const canonicalJson = (column: string) => `
  ${strictText(column)}
  AND json_valid(${column}) = 1
  AND json(${column}) = ${column}
`;

const isolateExistingControlledThreadValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly sql: string }>`
    SELECT sql FROM sqlite_schema
    WHERE type = 'trigger' AND name = 'agent_control_controlled_thread_event_total_validate'
  `;
  const source = rows[0]?.sql;
  const needle = "WHEN NEW.aggregate_kind = 'controlled-thread-reservation'";
  if (source === undefined || !source.includes(needle)) {
    return yield* Effect.die(
      new Error("migration 057 could not isolate controlled-thread event validation"),
    );
  }
  const verificationExclusion = `${needle}
      AND NOT COALESCE((
        typeof(NEW.payload_json) = 'text'
        AND json_valid(NEW.payload_json) = 1
        AND json_type(NEW.payload_json) = 'object'
        AND json_type(NEW.payload_json, '$.stageKind') = 'text'
        AND json_extract(NEW.payload_json, '$.stageKind') = 'verification'
      ), 0)`;
  yield* sql`DROP TRIGGER agent_control_controlled_thread_event_total_validate`;
  yield* sql.unsafe(source.replace(needle, verificationExclusion)).unprepared;
});

const createVerificationReservationBoundary = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TABLE agent_control_verification_thread_stream_catalog (
      controlled_thread_reservation_id TEXT PRIMARY KEY,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind = 'controlled-thread-reservation'),
      stream_version INTEGER NOT NULL CHECK (stream_version = 1),
      command_id TEXT NOT NULL,
      event_type TEXT NOT NULL CHECK (
        event_type = 'agentControl.controlledThreadReservation.prepared'
      ),
      thread_id TEXT NOT NULL UNIQUE CHECK (thread_id LIKE 't3-auto-reserved-thread-%'),
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL,
      stage_run_id TEXT NOT NULL UNIQUE,
      attempt_id TEXT NOT NULL UNIQUE,
      role_id TEXT NOT NULL CHECK (role_id = 'verifier'),
      stage_kind TEXT NOT NULL CHECK (stage_kind = 'verification'),
      stage_ordinal INTEGER NOT NULL CHECK (stage_ordinal = 3),
      attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal = 1),
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
      worktree_reservation_id TEXT NOT NULL,
      prepared_at TEXT NOT NULL,
      coordinator_command_id TEXT CHECK (coordinator_command_id IS NULL),
      coordinator_command_fingerprint TEXT CHECK (coordinator_command_fingerprint IS NULL),
      materializing_transition_command_id TEXT CHECK (materializing_transition_command_id IS NULL),
      materialization_command_id TEXT CHECK (materialization_command_id IS NULL),
      materialization_command_fingerprint TEXT CHECK (materialization_command_fingerprint IS NULL),
      lease_holder_id TEXT CHECK (lease_holder_id IS NULL),
      materializing_at TEXT CHECK (materializing_at IS NULL),
      bound_transition_command_id TEXT CHECK (bound_transition_command_id IS NULL),
      orchestration_result_sequence INTEGER CHECK (orchestration_result_sequence IS NULL),
      materialized_at TEXT CHECK (materialized_at IS NULL),
      bound_at TEXT CHECK (bound_at IS NULL),
      UNIQUE (
        project_id, task_id, task_revision, github_intake_sequence,
        source_identity_fingerprint, stage_kind, stage_ordinal, attempt_ordinal
      ),
      FOREIGN KEY (stage_run_id)
        REFERENCES agent_control_stage_run_states(stage_run_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (lease_id)
        REFERENCES agent_control_stage_run_lease_states(lease_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (worktree_reservation_id)
        REFERENCES agent_control_worktree_reservation_states(reservation_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (
        event_id, aggregate_kind, controlled_thread_reservation_id,
        stream_version, event_type, command_id
      ) REFERENCES agent_control_events(
        event_id, aggregate_kind, stream_id,
        stream_version, event_type, command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TABLE agent_control_verification_thread_reservation_states (
      controlled_thread_reservation_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL UNIQUE,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL,
      stage_run_id TEXT NOT NULL UNIQUE,
      attempt_id TEXT NOT NULL UNIQUE,
      role_id TEXT NOT NULL CHECK (role_id = 'verifier'),
      stage_kind TEXT NOT NULL CHECK (stage_kind = 'verification'),
      stage_ordinal INTEGER NOT NULL CHECK (stage_ordinal = 3),
      attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal = 1),
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
      worktree_reservation_id TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status = 'prepared'),
      revision INTEGER NOT NULL CHECK (revision = 1),
      last_event_sequence INTEGER NOT NULL UNIQUE CHECK (last_event_sequence >= 1),
      prepared_at TEXT NOT NULL,
      coordinator_command_id TEXT CHECK (coordinator_command_id IS NULL),
      coordinator_command_fingerprint TEXT CHECK (coordinator_command_fingerprint IS NULL),
      materializing_transition_command_id TEXT CHECK (materializing_transition_command_id IS NULL),
      materialization_command_id TEXT CHECK (materialization_command_id IS NULL),
      materialization_command_fingerprint TEXT CHECK (materialization_command_fingerprint IS NULL),
      lease_holder_id TEXT CHECK (lease_holder_id IS NULL),
      materializing_at TEXT CHECK (materializing_at IS NULL),
      bound_transition_command_id TEXT CHECK (bound_transition_command_id IS NULL),
      orchestration_result_sequence INTEGER CHECK (orchestration_result_sequence IS NULL),
      materialized_at TEXT CHECK (materialized_at IS NULL),
      bound_at TEXT CHECK (bound_at IS NULL),
      state_json TEXT NOT NULL,
      UNIQUE (
        project_id, task_id, stage_run_id, attempt_id, role_id,
        stage_ordinal, attempt_ordinal
      ),
      FOREIGN KEY (controlled_thread_reservation_id)
        REFERENCES agent_control_verification_thread_stream_catalog(
          controlled_thread_reservation_id
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (last_event_sequence)
        REFERENCES agent_control_events(sequence) ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `).unprepared;

  yield* sql`DROP VIEW agent_control_controlled_thread_stream_catalog_all`;
  yield* sql`DROP VIEW agent_control_controlled_thread_reservation_states_all`;
  yield* sql.unsafe(`
    CREATE VIEW agent_control_controlled_thread_stream_catalog_all AS
    SELECT * FROM agent_control_controlled_thread_stream_catalog
    UNION ALL SELECT * FROM agent_control_implementation_thread_stream_catalog
    UNION ALL SELECT * FROM agent_control_verification_thread_stream_catalog
  `).unprepared;
  yield* sql.unsafe(`
    CREATE VIEW agent_control_controlled_thread_reservation_states_all AS
    SELECT * FROM agent_control_controlled_thread_reservation_states
    UNION ALL SELECT * FROM agent_control_implementation_thread_reservation_states
    UNION ALL SELECT * FROM agent_control_verification_thread_reservation_states
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_catalog_storage_validate
    BEFORE INSERT ON agent_control_verification_thread_stream_catalog
    WHEN NOT COALESCE((
      ${[
        "controlled_thread_reservation_id",
        "event_id",
        "aggregate_kind",
        "command_id",
        "event_type",
        "thread_id",
        "project_id",
        "task_id",
        "stage_run_id",
        "attempt_id",
        "role_id",
        "stage_kind",
        "lease_id",
        "worktree_reservation_id",
      ]
        .map((column) => strictText(`NEW.${column}`))
        .join(" AND ")}
      AND ${sha256("NEW.source_identity_fingerprint")}
      AND ${[
        "stream_version",
        "task_revision",
        "github_intake_sequence",
        "stage_ordinal",
        "attempt_ordinal",
        "fence_token",
      ]
        .map((column) => positive(`NEW.${column}`))
        .join(" AND ")}
      AND ${timestamp("NEW.prepared_at")}
      AND NEW.coordinator_command_id IS NULL
      AND NEW.coordinator_command_fingerprint IS NULL
      AND NEW.materializing_transition_command_id IS NULL
      AND NEW.materialization_command_id IS NULL
      AND NEW.materialization_command_fingerprint IS NULL
      AND NEW.lease_holder_id IS NULL AND NEW.materializing_at IS NULL
      AND NEW.bound_transition_command_id IS NULL
      AND NEW.orchestration_result_sequence IS NULL
      AND NEW.materialized_at IS NULL AND NEW.bound_at IS NULL
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification reservation catalog storage'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_projection_storage_validate
    BEFORE INSERT ON agent_control_verification_thread_reservation_states
    WHEN NOT COALESCE((
      ${[
        "controlled_thread_reservation_id",
        "thread_id",
        "project_id",
        "task_id",
        "stage_run_id",
        "attempt_id",
        "role_id",
        "stage_kind",
        "lease_id",
        "worktree_reservation_id",
        "status",
      ]
        .map((column) => strictText(`NEW.${column}`))
        .join(" AND ")}
      AND ${sha256("NEW.source_identity_fingerprint")}
      AND ${[
        "task_revision",
        "github_intake_sequence",
        "stage_ordinal",
        "attempt_ordinal",
        "fence_token",
        "revision",
        "last_event_sequence",
      ]
        .map((column) => positive(`NEW.${column}`))
        .join(" AND ")}
      AND ${timestamp("NEW.prepared_at")}
      AND ${canonicalJson("NEW.state_json")}
      AND NEW.coordinator_command_id IS NULL
      AND NEW.coordinator_command_fingerprint IS NULL
      AND NEW.materializing_transition_command_id IS NULL
      AND NEW.materialization_command_id IS NULL
      AND NEW.materialization_command_fingerprint IS NULL
      AND NEW.lease_holder_id IS NULL AND NEW.materializing_at IS NULL
      AND NEW.bound_transition_command_id IS NULL
      AND NEW.orchestration_result_sequence IS NULL
      AND NEW.materialized_at IS NULL AND NEW.bound_at IS NULL
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification reservation projection storage'); END
  `).unprepared;
});

const createVerificationEventAndProjectionValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_stage_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'stage-run'
      AND json_extract(NEW.payload_json, '$.stageKind') = 'verification'
      AND NOT COALESCE((
        ${strictText("NEW.event_id")}
        AND NEW.stream_version = 1
        AND NEW.event_type = 'agentControl.stageRun.prepared'
        AND ${timestamp("NEW.occurred_at")}
        AND ${strictText("NEW.command_id")}
        AND NEW.causation_event_id IS NULL
        AND NEW.correlation_id IS NEW.command_id
        AND NEW.actor_authority = 'controller'
        AND ${canonicalJson("NEW.payload_json")}
        AND NEW.metadata_json = '{"schemaVersion":1}'
        AND NEW.stream_id IS json_extract(NEW.payload_json, '$.stageRunId')
        AND NEW.occurred_at IS json_extract(NEW.payload_json, '$.preparedAt')
        AND json_extract(NEW.payload_json, '$.roleId') = 'verifier'
        AND json_extract(NEW.payload_json, '$.stageKind') = 'verification'
        AND json_extract(NEW.payload_json, '$.stageOrdinal') = 3
        AND json_extract(NEW.payload_json, '$.attemptOrdinal') = 1
        AND json_extract(NEW.payload_json, '$.status') = 'prepared'
        AND NEW.payload_json = json_object(
          'projectId', json_extract(NEW.payload_json, '$.projectId'),
          'taskId', json_extract(NEW.payload_json, '$.taskId'),
          'stageRunId', json_extract(NEW.payload_json, '$.stageRunId'),
          'attemptId', json_extract(NEW.payload_json, '$.attemptId'),
          'roleId', 'verifier', 'stageKind', 'verification',
          'stageOrdinal', 3, 'attemptOrdinal', 1, 'status', 'prepared',
          'taskRevision', json_extract(NEW.payload_json, '$.taskRevision'),
          'githubIntakeSequence', json_extract(NEW.payload_json, '$.githubIntakeSequence'),
          'sourceIdentityFingerprint',
            json_extract(NEW.payload_json, '$.sourceIdentityFingerprint'),
          'preparedAt', NEW.occurred_at
        )
        AND EXISTS (
          SELECT 1
          FROM agent_control_implementation_result_evidence result
          JOIN agent_control_implementation_stage_finalization_receipts receipt
            ON receipt.result_evidence_id = result.result_evidence_id
          JOIN agent_control_implementation_stage_finalization_markers marker
            ON marker.result_evidence_id = result.result_evidence_id
          WHERE result.outcome = 'succeeded'
            AND receipt.status = 'accepted'
            AND result.project_id IS json_extract(NEW.payload_json, '$.projectId')
            AND result.task_id IS json_extract(NEW.payload_json, '$.taskId')
            AND result.task_revision IS json_extract(NEW.payload_json, '$.taskRevision')
            AND result.github_intake_sequence IS
              json_extract(NEW.payload_json, '$.githubIntakeSequence')
            AND result.source_identity_fingerprint IS
              json_extract(NEW.payload_json, '$.sourceIdentityFingerprint')
        )
        AND NOT EXISTS (
          SELECT 1 FROM agent_control_stage_run_states stage
          WHERE stage.project_id IS json_extract(NEW.payload_json, '$.projectId')
            AND stage.task_id IS json_extract(NEW.payload_json, '$.taskId')
            AND stage.stage_kind = 'verification'
        )
      ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification stage prepared event'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_stage_projection_insert_validate
    BEFORE INSERT ON agent_control_stage_run_states
    WHEN NEW.stage_kind = 'verification'
      AND NOT COALESCE((
        NEW.role_id = 'verifier' AND NEW.stage_ordinal = 3 AND NEW.attempt_ordinal = 1
        AND NEW.status = 'prepared' AND NEW.revision = 1
        AND NEW.created_at IS NEW.updated_at
        AND ${sha256("NEW.source_identity_fingerprint")}
        AND ${timestamp("NEW.created_at")}
        AND ${canonicalJson("NEW.state_json")}
        AND NEW.state_json = json_object(
          'schemaVersion', 1, 'projectId', NEW.project_id, 'taskId', NEW.task_id,
          'stageRunId', NEW.stage_run_id, 'attemptId', NEW.attempt_id,
          'roleId', 'verifier', 'stageKind', 'verification',
          'stageOrdinal', 3, 'attemptOrdinal', 1, 'status', 'prepared',
          'taskRevision', NEW.task_revision,
          'githubIntakeSequence', NEW.github_intake_sequence,
          'sourceIdentityFingerprint', NEW.source_identity_fingerprint,
          'createdAt', NEW.created_at, 'updatedAt', NEW.updated_at,
          'revision', 1, 'sequence', NEW.last_event_sequence
        )
        AND (SELECT count(*) FROM agent_control_events event
          WHERE event.stream_id IS NEW.stage_run_id
            AND event.sequence IS NEW.last_event_sequence
            AND event.stream_version = 1
            AND event.event_type = 'agentControl.stageRun.prepared') = 1
      ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification stage projection'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_stage_projection_update_reject
    BEFORE UPDATE ON agent_control_stage_run_states
    WHEN OLD.stage_kind = 'verification' OR NEW.stage_kind = 'verification'
    BEGIN SELECT RAISE(ABORT, 'verification stage transitions are not admitted'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_lease_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'stage-run-lease'
      AND EXISTS (
        SELECT 1 FROM agent_control_stage_run_states stage
        WHERE stage.stage_run_id IS json_extract(NEW.payload_json, '$.stageRunId')
          AND stage.stage_kind = 'verification'
      )
      AND NOT COALESCE((
        ${strictText("NEW.event_id")}
        AND NEW.event_type = 'agentControl.stageRunLease.reserved'
        AND ${timestamp("NEW.occurred_at")}
        AND ${strictText("NEW.command_id")}
        AND NEW.causation_event_id IS NULL
        AND NEW.correlation_id IS NEW.command_id
        AND NEW.actor_authority = 'controller'
        AND ${canonicalJson("NEW.payload_json")}
        AND NEW.metadata_json = '{"schemaVersion":1}'
        AND NEW.stream_id IS json_extract(NEW.payload_json, '$.leaseId')
        AND NEW.occurred_at IS json_extract(NEW.payload_json, '$.acquiredAt')
        AND NEW.occurred_at IS json_extract(NEW.payload_json, '$.renewedAt')
        AND json_type(NEW.payload_json, '$.fenceToken') = 'integer'
        AND json_extract(NEW.payload_json, '$.fenceToken') >= 3
        AND NEW.payload_json = json_object(
          'leaseId', json_extract(NEW.payload_json, '$.leaseId'),
          'projectId', json_extract(NEW.payload_json, '$.projectId'),
          'taskId', json_extract(NEW.payload_json, '$.taskId'),
          'stageRunId', json_extract(NEW.payload_json, '$.stageRunId'),
          'attemptId', json_extract(NEW.payload_json, '$.attemptId'),
          'taskRevision', json_extract(NEW.payload_json, '$.taskRevision'),
          'githubIntakeSequence', json_extract(NEW.payload_json, '$.githubIntakeSequence'),
          'sourceIdentityFingerprint',
            json_extract(NEW.payload_json, '$.sourceIdentityFingerprint'),
          'holderId', json_extract(NEW.payload_json, '$.holderId'),
          'fenceToken', json_extract(NEW.payload_json, '$.fenceToken'),
          'acquiredAt', NEW.occurred_at, 'renewedAt', NEW.occurred_at,
          'expiresAt', json_extract(NEW.payload_json, '$.expiresAt')
        )
        AND (SELECT count(*)
          FROM agent_control_stage_run_lease_states lease
          JOIN agent_control_implementation_result_evidence result
            ON result.lease_id IS lease.lease_id
          JOIN agent_control_events released ON released.event_id IS result.lease_event_id
          JOIN agent_control_stage_run_states stage
            ON stage.stage_run_id IS json_extract(NEW.payload_json, '$.stageRunId')
          WHERE lease.lease_id IS NEW.stream_id
            AND NEW.stream_version = lease.revision + 1
            AND lease.status = 'released'
            AND lease.stage_run_id IS result.stage_run_id
            AND lease.attempt_id IS result.attempt_id
            AND lease.holder_id IS json_extract(NEW.payload_json, '$.holderId')
            AND lease.fence_token + 1 IS json_extract(NEW.payload_json, '$.fenceToken')
            AND lease.last_event_sequence IS result.lease_event_sequence
            AND released.event_type = 'agentControl.stageRunLease.releasedAfterImplementation'
            AND result.outcome = 'succeeded'
            AND stage.status = 'prepared' AND stage.revision = 1
            AND stage.project_id IS result.project_id AND stage.task_id IS result.task_id
            AND stage.task_revision IS result.task_revision
            AND stage.github_intake_sequence IS result.github_intake_sequence
            AND stage.source_identity_fingerprint IS result.source_identity_fingerprint
            AND json_extract(NEW.payload_json, '$.projectId') IS result.project_id
            AND json_extract(NEW.payload_json, '$.taskId') IS result.task_id
            AND json_extract(NEW.payload_json, '$.taskRevision') IS result.task_revision
            AND json_extract(NEW.payload_json, '$.githubIntakeSequence') IS
              result.github_intake_sequence
            AND json_extract(NEW.payload_json, '$.sourceIdentityFingerprint') IS
              result.source_identity_fingerprint
        ) = 1
      ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification lease reservation event'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_lease_projection_insert_reject
    BEFORE INSERT ON agent_control_stage_run_lease_states
    WHEN EXISTS (
      SELECT 1 FROM agent_control_stage_run_states stage
      WHERE stage.stage_run_id = NEW.stage_run_id AND stage.stage_kind = 'verification'
    )
    BEGIN SELECT RAISE(ABORT, 'verification must reuse the task-bound lease'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_lease_projection_update_validate
    BEFORE UPDATE ON agent_control_stage_run_lease_states
    WHEN EXISTS (
      SELECT 1 FROM agent_control_stage_run_states stage
      WHERE stage.stage_run_id = NEW.stage_run_id AND stage.stage_kind = 'verification'
    )
      AND NOT COALESCE((
        OLD.status = 'released' AND NEW.status = 'reserved'
        AND NEW.released_at IS NULL
        AND NEW.lease_id IS OLD.lease_id
        AND NEW.project_id IS OLD.project_id AND NEW.task_id IS OLD.task_id
        AND NEW.task_revision IS OLD.task_revision
        AND NEW.github_intake_sequence IS OLD.github_intake_sequence
        AND NEW.source_identity_fingerprint IS OLD.source_identity_fingerprint
        AND NEW.holder_id IS OLD.holder_id
        AND NEW.fence_token = OLD.fence_token + 1 AND NEW.fence_token >= 3
        AND NEW.revision = OLD.revision + 1
        AND NEW.acquired_at IS NEW.renewed_at
        AND ${timestamp("NEW.acquired_at")} AND ${timestamp("NEW.expires_at")}
        AND (SELECT count(*) FROM agent_control_events event
          WHERE event.stream_id IS NEW.lease_id
            AND event.sequence IS NEW.last_event_sequence
            AND event.stream_version IS NEW.revision
            AND event.event_type = 'agentControl.stageRunLease.reserved'
            AND json_extract(event.payload_json, '$.stageRunId') IS NEW.stage_run_id
            AND json_extract(event.payload_json, '$.attemptId') IS NEW.attempt_id
            AND json_extract(event.payload_json, '$.holderId') IS NEW.holder_id
            AND json_extract(event.payload_json, '$.fenceToken') IS NEW.fence_token) = 1
      ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification lease projection'); END
  `).unprepared;
});

const createVerificationReservationValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_catalog_validate
    BEFORE INSERT ON agent_control_verification_thread_stream_catalog
    WHEN NOT COALESCE((
      NEW.aggregate_kind = 'controlled-thread-reservation'
      AND NEW.stream_version = 1
      AND NEW.event_type = 'agentControl.controlledThreadReservation.prepared'
      AND NEW.role_id = 'verifier' AND NEW.stage_kind = 'verification'
      AND NEW.stage_ordinal = 3 AND NEW.attempt_ordinal = 1
      AND NEW.fence_token >= 3
      AND NOT EXISTS (
        SELECT 1 FROM agent_control_controlled_thread_stream_catalog_all old
        WHERE old.controlled_thread_reservation_id IS NEW.controlled_thread_reservation_id
          OR old.thread_id IS NEW.thread_id
      )
      AND (SELECT count(*) FROM agent_control_stage_run_states stage
        WHERE stage.stage_run_id IS NEW.stage_run_id
          AND stage.project_id IS NEW.project_id AND stage.task_id IS NEW.task_id
          AND stage.attempt_id IS NEW.attempt_id AND stage.role_id = 'verifier'
          AND stage.stage_kind = 'verification' AND stage.stage_ordinal = 3
          AND stage.attempt_ordinal = 1 AND stage.status = 'prepared'
          AND stage.revision = 1 AND stage.task_revision IS NEW.task_revision
          AND stage.github_intake_sequence IS NEW.github_intake_sequence
          AND stage.source_identity_fingerprint IS NEW.source_identity_fingerprint) = 1
      AND (SELECT count(*) FROM agent_control_stage_run_lease_states lease
        WHERE lease.lease_id IS NEW.lease_id AND lease.project_id IS NEW.project_id
          AND lease.task_id IS NEW.task_id AND lease.stage_run_id IS NEW.stage_run_id
          AND lease.attempt_id IS NEW.attempt_id AND lease.status = 'reserved'
          AND lease.fence_token IS NEW.fence_token
          AND lease.task_revision IS NEW.task_revision
          AND lease.github_intake_sequence IS NEW.github_intake_sequence
          AND lease.source_identity_fingerprint IS NEW.source_identity_fingerprint) = 1
      AND (SELECT count(*) FROM agent_control_worktree_reservation_states worktree
        JOIN agent_control_implementation_result_evidence result
          ON result.worktree_reservation_id IS worktree.reservation_id
        WHERE worktree.reservation_id IS NEW.worktree_reservation_id
          AND result.project_id IS NEW.project_id AND result.task_id IS NEW.task_id
          AND result.task_revision IS NEW.task_revision
          AND result.github_intake_sequence IS NEW.github_intake_sequence
          AND result.source_identity_fingerprint IS NEW.source_identity_fingerprint
          AND result.worktree_ownership_fingerprint IS worktree.ownership_fingerprint) = 1
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification reservation catalog binding'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'controlled-thread-reservation'
      AND json_extract(NEW.payload_json, '$.stageKind') = 'verification'
      AND NOT COALESCE((
        ${strictText("NEW.event_id")}
        AND NEW.stream_version = 1
        AND NEW.event_type = 'agentControl.controlledThreadReservation.prepared'
        AND ${timestamp("NEW.occurred_at")}
        AND ${strictText("NEW.command_id")}
        AND NEW.causation_event_id IS NULL
        AND NEW.correlation_id IS NEW.command_id
        AND NEW.actor_authority = 'controller'
        AND ${canonicalJson("NEW.payload_json")}
        AND NEW.metadata_json = '{"schemaVersion":1}'
        AND NEW.stream_id IS
          json_extract(NEW.payload_json, '$.controlledThreadReservationId')
        AND NEW.occurred_at IS json_extract(NEW.payload_json, '$.preparedAt')
        AND json_extract(NEW.payload_json, '$.roleId') = 'verifier'
        AND json_extract(NEW.payload_json, '$.stageKind') = 'verification'
        AND json_extract(NEW.payload_json, '$.stageOrdinal') = 3
        AND json_extract(NEW.payload_json, '$.attemptOrdinal') = 1
        AND json_extract(NEW.payload_json, '$.status') = 'prepared'
        AND NEW.payload_json = json_object(
          'controlledThreadReservationId', NEW.stream_id,
          'threadId', json_extract(NEW.payload_json, '$.threadId'),
          'projectId', json_extract(NEW.payload_json, '$.projectId'),
          'taskId', json_extract(NEW.payload_json, '$.taskId'),
          'taskRevision', json_extract(NEW.payload_json, '$.taskRevision'),
          'githubIntakeSequence', json_extract(NEW.payload_json, '$.githubIntakeSequence'),
          'sourceIdentityFingerprint',
            json_extract(NEW.payload_json, '$.sourceIdentityFingerprint'),
          'stageRunId', json_extract(NEW.payload_json, '$.stageRunId'),
          'attemptId', json_extract(NEW.payload_json, '$.attemptId'),
          'roleId', 'verifier', 'stageKind', 'verification',
          'stageOrdinal', 3, 'attemptOrdinal', 1,
          'leaseId', json_extract(NEW.payload_json, '$.leaseId'),
          'fenceToken', json_extract(NEW.payload_json, '$.fenceToken'),
          'worktreeReservationId',
            json_extract(NEW.payload_json, '$.worktreeReservationId'),
          'status', 'prepared', 'preparedAt', NEW.occurred_at
        )
        AND (SELECT count(*)
          FROM agent_control_verification_thread_stream_catalog catalog
          WHERE catalog.controlled_thread_reservation_id IS NEW.stream_id
            AND catalog.event_id IS NEW.event_id
            AND catalog.aggregate_kind IS NEW.aggregate_kind
            AND catalog.stream_version IS NEW.stream_version
            AND catalog.command_id IS NEW.command_id
            AND catalog.event_type IS NEW.event_type
            AND catalog.thread_id IS json_extract(NEW.payload_json, '$.threadId')
            AND catalog.project_id IS json_extract(NEW.payload_json, '$.projectId')
            AND catalog.task_id IS json_extract(NEW.payload_json, '$.taskId')
            AND catalog.task_revision IS json_extract(NEW.payload_json, '$.taskRevision')
            AND catalog.github_intake_sequence IS
              json_extract(NEW.payload_json, '$.githubIntakeSequence')
            AND catalog.source_identity_fingerprint IS
              json_extract(NEW.payload_json, '$.sourceIdentityFingerprint')
            AND catalog.stage_run_id IS json_extract(NEW.payload_json, '$.stageRunId')
            AND catalog.attempt_id IS json_extract(NEW.payload_json, '$.attemptId')
            AND catalog.role_id IS json_extract(NEW.payload_json, '$.roleId')
            AND catalog.stage_kind IS json_extract(NEW.payload_json, '$.stageKind')
            AND catalog.stage_ordinal IS json_extract(NEW.payload_json, '$.stageOrdinal')
            AND catalog.attempt_ordinal IS json_extract(NEW.payload_json, '$.attemptOrdinal')
            AND catalog.lease_id IS json_extract(NEW.payload_json, '$.leaseId')
            AND catalog.fence_token IS json_extract(NEW.payload_json, '$.fenceToken')
            AND catalog.worktree_reservation_id IS
              json_extract(NEW.payload_json, '$.worktreeReservationId')
            AND catalog.prepared_at IS json_extract(NEW.payload_json, '$.preparedAt')
            AND catalog.coordinator_command_id IS NULL
            AND catalog.coordinator_command_fingerprint IS NULL
            AND catalog.materializing_transition_command_id IS NULL
            AND catalog.materialization_command_id IS NULL
            AND catalog.materialization_command_fingerprint IS NULL
            AND catalog.lease_holder_id IS NULL AND catalog.materializing_at IS NULL
            AND catalog.bound_transition_command_id IS NULL
            AND catalog.orchestration_result_sequence IS NULL
            AND catalog.materialized_at IS NULL AND catalog.bound_at IS NULL) = 1
      ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification reservation event'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_projection_validate
    BEFORE INSERT ON agent_control_verification_thread_reservation_states
    WHEN NOT COALESCE((
      NEW.status = 'prepared' AND NEW.revision = 1
      AND NEW.role_id = 'verifier' AND NEW.stage_kind = 'verification'
      AND NEW.stage_ordinal = 3 AND NEW.attempt_ordinal = 1
      AND NEW.fence_token >= 3
      AND NEW.state_json = json_object(
        'schemaVersion', 1,
        'controlledThreadReservationId', NEW.controlled_thread_reservation_id,
        'threadId', NEW.thread_id, 'projectId', NEW.project_id, 'taskId', NEW.task_id,
        'taskRevision', NEW.task_revision,
        'githubIntakeSequence', NEW.github_intake_sequence,
        'sourceIdentityFingerprint', NEW.source_identity_fingerprint,
        'stageRunId', NEW.stage_run_id, 'attemptId', NEW.attempt_id,
        'roleId', 'verifier', 'stageKind', 'verification',
        'stageOrdinal', 3, 'attemptOrdinal', 1,
        'leaseId', NEW.lease_id, 'fenceToken', NEW.fence_token,
        'worktreeReservationId', NEW.worktree_reservation_id,
        'status', 'prepared', 'revision', 1,
        'sequence', NEW.last_event_sequence, 'preparedAt', NEW.prepared_at
      )
      AND (SELECT count(*)
        FROM agent_control_verification_thread_stream_catalog catalog
        JOIN agent_control_events event ON event.event_id IS catalog.event_id
        WHERE catalog.controlled_thread_reservation_id IS
            NEW.controlled_thread_reservation_id
          AND catalog.thread_id IS NEW.thread_id AND catalog.project_id IS NEW.project_id
          AND catalog.task_id IS NEW.task_id AND catalog.stage_run_id IS NEW.stage_run_id
          AND catalog.attempt_id IS NEW.attempt_id AND catalog.lease_id IS NEW.lease_id
          AND catalog.fence_token IS NEW.fence_token
          AND catalog.worktree_reservation_id IS NEW.worktree_reservation_id
          AND event.sequence IS NEW.last_event_sequence) = 1
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification reservation projection'); END
  `).unprepared;

  for (const operation of ["UPDATE", "DELETE"] as const) {
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_thread_catalog_no_${operation.toLowerCase()}
      BEFORE ${operation} ON agent_control_verification_thread_stream_catalog
      BEGIN SELECT RAISE(ABORT, 'verification reservation catalog is immutable'); END
    `).unprepared;
  }
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_projection_no_update
    BEFORE UPDATE ON agent_control_verification_thread_reservation_states
    BEGIN SELECT RAISE(ABORT, 'verification reservation transitions are not admitted'); END
  `).unprepared;
});

const createAdmissionCompanions = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TABLE agent_control_verification_admission_evidence (
      admission_evidence_id TEXT PRIMARY KEY,
      receipt_id TEXT UNIQUE NOT NULL,
      marker_id TEXT UNIQUE NOT NULL,
      admission_command_id TEXT UNIQUE NOT NULL,
      admission_fingerprint TEXT UNIQUE NOT NULL,
      evidence_json TEXT UNIQUE NOT NULL,
      implementation_result_evidence_id TEXT UNIQUE NOT NULL,
      implementation_result_json TEXT UNIQUE NOT NULL,
      implementation_finalization_fingerprint TEXT UNIQUE NOT NULL,
      implementation_finalization_receipt_id TEXT UNIQUE NOT NULL,
      implementation_finalization_marker_id TEXT UNIQUE NOT NULL,
      handoff_id TEXT UNIQUE NOT NULL,
      handoff_fingerprint TEXT UNIQUE NOT NULL,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL,
      repository_display TEXT NOT NULL,
      source_revision TEXT NOT NULL,
      worktree_reservation_id TEXT NOT NULL,
      worktree_event_id TEXT NOT NULL,
      worktree_event_sequence INTEGER NOT NULL CHECK (worktree_event_sequence >= 1),
      worktree_event_stream_version INTEGER NOT NULL CHECK (worktree_event_stream_version >= 1),
      worktree_ownership_fingerprint TEXT NOT NULL,
      implementation_stage_run_id TEXT UNIQUE NOT NULL,
      implementation_attempt_id TEXT UNIQUE NOT NULL,
      implementation_controlled_thread_reservation_id TEXT UNIQUE NOT NULL,
      implementation_thread_id TEXT UNIQUE NOT NULL,
      implementation_terminal_stage_event_id TEXT UNIQUE NOT NULL,
      implementation_terminal_stage_event_sequence INTEGER UNIQUE NOT NULL,
      implementation_terminal_stage_event_stream_version INTEGER NOT NULL CHECK (
        implementation_terminal_stage_event_stream_version = 3
      ),
      lease_id TEXT NOT NULL,
      lease_holder_id TEXT NOT NULL,
      implementation_fence_token INTEGER NOT NULL CHECK (implementation_fence_token >= 2),
      implementation_lease_release_event_id TEXT UNIQUE NOT NULL,
      implementation_lease_release_event_sequence INTEGER UNIQUE NOT NULL,
      implementation_lease_release_stream_version INTEGER NOT NULL CHECK (
        implementation_lease_release_stream_version >= 2
      ),
      verification_stage_run_id TEXT UNIQUE NOT NULL,
      verification_attempt_id TEXT UNIQUE NOT NULL,
      verification_fence_token INTEGER NOT NULL CHECK (verification_fence_token >= 3),
      verification_controlled_thread_reservation_id TEXT UNIQUE NOT NULL,
      verification_thread_id TEXT UNIQUE NOT NULL,
      verification_stage_event_id TEXT UNIQUE NOT NULL,
      verification_stage_event_sequence INTEGER UNIQUE NOT NULL,
      verification_stage_event_stream_version INTEGER NOT NULL CHECK (
        verification_stage_event_stream_version = 1
      ),
      verification_lease_event_id TEXT UNIQUE NOT NULL,
      verification_lease_event_sequence INTEGER UNIQUE NOT NULL,
      verification_lease_event_stream_version INTEGER NOT NULL,
      verification_reservation_event_id TEXT UNIQUE NOT NULL,
      verification_reservation_event_sequence INTEGER UNIQUE NOT NULL,
      verification_reservation_event_stream_version INTEGER NOT NULL CHECK (
        verification_reservation_event_stream_version = 1
      ),
      lease_duration_ms INTEGER NOT NULL CHECK (lease_duration_ms >= 1),
      task_history_json TEXT NOT NULL,
      task_history_digest TEXT NOT NULL,
      task_history_event_count INTEGER NOT NULL CHECK (task_history_event_count >= 1),
      worktree_history_json TEXT NOT NULL,
      worktree_history_digest TEXT NOT NULL,
      worktree_history_event_count INTEGER NOT NULL CHECK (worktree_history_event_count >= 1),
      stage_history_json TEXT NOT NULL,
      stage_history_digest TEXT NOT NULL,
      stage_history_event_count INTEGER NOT NULL CHECK (stage_history_event_count >= 3),
      lease_history_json TEXT NOT NULL,
      lease_history_digest TEXT NOT NULL,
      lease_history_event_count INTEGER NOT NULL CHECK (lease_history_event_count >= 3),
      reservation_history_json TEXT NOT NULL,
      reservation_history_digest TEXT NOT NULL,
      reservation_history_event_count INTEGER NOT NULL CHECK (reservation_history_event_count >= 4),
      orchestration_history_json TEXT NOT NULL,
      orchestration_history_digest TEXT NOT NULL,
      orchestration_history_event_count INTEGER NOT NULL CHECK (
        orchestration_history_event_count >= 1
      ),
      admitted_at TEXT NOT NULL,
      CHECK (verification_fence_token = implementation_fence_token + 1),
      FOREIGN KEY (implementation_result_evidence_id)
        REFERENCES agent_control_implementation_result_evidence(result_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (implementation_finalization_receipt_id)
        REFERENCES agent_control_implementation_stage_finalization_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (implementation_finalization_marker_id)
        REFERENCES agent_control_implementation_stage_finalization_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (worktree_event_id, worktree_reservation_id, worktree_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (verification_stage_event_id, verification_stage_run_id,
        verification_stage_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (verification_lease_event_id, lease_id,
        verification_lease_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (verification_reservation_event_id,
        verification_controlled_thread_reservation_id,
        verification_reservation_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (receipt_id)
        REFERENCES agent_control_verification_admission_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id)
        REFERENCES agent_control_verification_admission_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TABLE agent_control_verification_admission_receipts (
      receipt_id TEXT PRIMARY KEY,
      marker_id TEXT UNIQUE NOT NULL,
      admission_command_id TEXT UNIQUE NOT NULL,
      admission_fingerprint TEXT UNIQUE NOT NULL,
      admission_evidence_id TEXT UNIQUE NOT NULL,
      implementation_result_evidence_id TEXT UNIQUE NOT NULL,
      verification_stage_event_id TEXT UNIQUE NOT NULL,
      verification_stage_event_sequence INTEGER UNIQUE NOT NULL,
      verification_lease_event_id TEXT UNIQUE NOT NULL,
      verification_lease_event_sequence INTEGER UNIQUE NOT NULL,
      verification_reservation_event_id TEXT UNIQUE NOT NULL,
      verification_reservation_event_sequence INTEGER UNIQUE NOT NULL,
      status TEXT NOT NULL CHECK (status = 'accepted'),
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (admission_evidence_id)
        REFERENCES agent_control_verification_admission_evidence(admission_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id)
        REFERENCES agent_control_verification_admission_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TABLE agent_control_verification_admission_markers (
      marker_id TEXT PRIMARY KEY,
      marker_fingerprint TEXT UNIQUE NOT NULL,
      receipt_id TEXT UNIQUE NOT NULL,
      admission_command_id TEXT UNIQUE NOT NULL,
      admission_fingerprint TEXT UNIQUE NOT NULL,
      admission_evidence_id TEXT UNIQUE NOT NULL,
      implementation_result_evidence_id TEXT UNIQUE NOT NULL,
      committed_at TEXT NOT NULL,
      FOREIGN KEY (receipt_id)
        REFERENCES agent_control_verification_admission_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (admission_evidence_id)
        REFERENCES agent_control_verification_admission_evidence(admission_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `).unprepared;
});

const createAdmissionStorageValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_admission_evidence_storage_validate
    BEFORE INSERT ON agent_control_verification_admission_evidence
    WHEN NOT COALESCE((
      ${[
        "admission_evidence_id",
        "receipt_id",
        "marker_id",
        "admission_command_id",
        "implementation_result_evidence_id",
        "implementation_finalization_receipt_id",
        "implementation_finalization_marker_id",
        "handoff_id",
        "project_id",
        "task_id",
        "repository_display",
        "source_revision",
        "worktree_reservation_id",
        "worktree_event_id",
        "implementation_stage_run_id",
        "implementation_attempt_id",
        "implementation_controlled_thread_reservation_id",
        "implementation_thread_id",
        "implementation_terminal_stage_event_id",
        "lease_id",
        "lease_holder_id",
        "implementation_lease_release_event_id",
        "verification_stage_run_id",
        "verification_attempt_id",
        "verification_controlled_thread_reservation_id",
        "verification_thread_id",
        "verification_stage_event_id",
        "verification_lease_event_id",
        "verification_reservation_event_id",
      ]
        .map((column) => strictText(`NEW.${column}`))
        .join(" AND ")}
      AND ${[
        "admission_fingerprint",
        "implementation_finalization_fingerprint",
        "handoff_fingerprint",
        "source_identity_fingerprint",
        "worktree_ownership_fingerprint",
        "task_history_digest",
        "worktree_history_digest",
        "stage_history_digest",
        "lease_history_digest",
        "reservation_history_digest",
        "orchestration_history_digest",
      ]
        .map((column) => sha256(`NEW.${column}`))
        .join(" AND ")}
      AND ${[
        "evidence_json",
        "implementation_result_json",
        "task_history_json",
        "worktree_history_json",
        "stage_history_json",
        "lease_history_json",
        "reservation_history_json",
        "orchestration_history_json",
      ]
        .map((column) => canonicalJson(`NEW.${column}`))
        .join(" AND ")}
      AND ${[
        "task_revision",
        "github_intake_sequence",
        "worktree_event_sequence",
        "worktree_event_stream_version",
        "implementation_terminal_stage_event_sequence",
        "implementation_terminal_stage_event_stream_version",
        "implementation_fence_token",
        "implementation_lease_release_event_sequence",
        "implementation_lease_release_stream_version",
        "verification_fence_token",
        "verification_stage_event_sequence",
        "verification_stage_event_stream_version",
        "verification_lease_event_sequence",
        "verification_lease_event_stream_version",
        "verification_reservation_event_sequence",
        "verification_reservation_event_stream_version",
        "lease_duration_ms",
        "task_history_event_count",
        "worktree_history_event_count",
        "stage_history_event_count",
        "lease_history_event_count",
        "reservation_history_event_count",
        "orchestration_history_event_count",
      ]
        .map((column) => positive(`NEW.${column}`))
        .join(" AND ")}
      AND ${timestamp("NEW.admitted_at")}
      AND json_type(NEW.task_history_json) = 'array'
      AND json_type(NEW.worktree_history_json) = 'array'
      AND json_type(NEW.stage_history_json) = 'array'
      AND json_type(NEW.lease_history_json) = 'array'
      AND json_type(NEW.reservation_history_json) = 'array'
      AND json_type(NEW.orchestration_history_json) = 'array'
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification admission evidence storage'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_admission_receipt_storage_validate
    BEFORE INSERT ON agent_control_verification_admission_receipts
    WHEN NOT COALESCE((
      ${[
        "receipt_id",
        "marker_id",
        "admission_command_id",
        "admission_evidence_id",
        "implementation_result_evidence_id",
        "verification_stage_event_id",
        "verification_lease_event_id",
        "verification_reservation_event_id",
        "status",
      ]
        .map((column) => strictText(`NEW.${column}`))
        .join(" AND ")}
      AND ${sha256("NEW.admission_fingerprint")}
      AND ${positive("NEW.verification_stage_event_sequence")}
      AND ${positive("NEW.verification_lease_event_sequence")}
      AND ${positive("NEW.verification_reservation_event_sequence")}
      AND ${timestamp("NEW.accepted_at")}
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification admission receipt storage'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_admission_marker_storage_validate
    BEFORE INSERT ON agent_control_verification_admission_markers
    WHEN NOT COALESCE((
      ${[
        "marker_id",
        "receipt_id",
        "admission_command_id",
        "admission_evidence_id",
        "implementation_result_evidence_id",
      ]
        .map((column) => strictText(`NEW.${column}`))
        .join(" AND ")}
      AND ${sha256("NEW.marker_fingerprint")}
      AND ${sha256("NEW.admission_fingerprint")}
      AND ${timestamp("NEW.committed_at")}
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification admission marker storage'); END
  `).unprepared;
});

const createAdmissionCrossValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_admission_evidence_validate
    BEFORE INSERT ON agent_control_verification_admission_evidence
    WHEN NOT COALESCE((
      json_extract(NEW.evidence_json, '$.schemaVersion') = 1
      AND json_extract(NEW.evidence_json, '$.admissionEvidenceId') IS NEW.admission_evidence_id
      AND json_extract(NEW.evidence_json, '$.admissionCommandId') IS NEW.admission_command_id
      AND json_extract(NEW.evidence_json, '$.admissionFingerprint') IS
        NEW.admission_fingerprint
      AND json_extract(NEW.evidence_json, '$.implementation.resultEvidenceId') IS
        NEW.implementation_result_evidence_id
      AND json_extract(NEW.evidence_json, '$.implementation.handoffId') IS NEW.handoff_id
      AND json_extract(NEW.evidence_json, '$.verification.stageRunId') IS
        NEW.verification_stage_run_id
      AND json_extract(NEW.evidence_json, '$.verification.attemptId') IS
        NEW.verification_attempt_id
      AND json_extract(NEW.evidence_json, '$.verification.leaseId') IS NEW.lease_id
      AND json_extract(NEW.evidence_json, '$.verification.leaseHolderId') IS
        NEW.lease_holder_id
      AND json_extract(NEW.evidence_json, '$.verification.fenceToken') IS
        NEW.verification_fence_token
      AND json_extract(NEW.evidence_json, '$.verification.controlledThreadReservationId') IS
        NEW.verification_controlled_thread_reservation_id
      AND json_extract(NEW.evidence_json, '$.verification.threadId') IS
        NEW.verification_thread_id
      AND json_extract(NEW.evidence_json, '$.verification.stageEventId') IS
        NEW.verification_stage_event_id
      AND json_extract(NEW.evidence_json, '$.verification.leaseEventId') IS
        NEW.verification_lease_event_id
      AND json_extract(NEW.evidence_json, '$.verification.reservationEventId') IS
        NEW.verification_reservation_event_id
      AND json_extract(NEW.evidence_json, '$.histories.task') IS NEW.task_history_json
      AND json_extract(NEW.evidence_json, '$.histories.worktree') IS NEW.worktree_history_json
      AND json_extract(NEW.evidence_json, '$.histories.stage') IS NEW.stage_history_json
      AND json_extract(NEW.evidence_json, '$.histories.lease') IS NEW.lease_history_json
      AND json_extract(NEW.evidence_json, '$.histories.reservation') IS
        NEW.reservation_history_json
      AND json_extract(NEW.evidence_json, '$.histories.orchestration') IS
        NEW.orchestration_history_json
      AND json_array_length(NEW.task_history_json) = NEW.task_history_event_count
      AND json_array_length(NEW.worktree_history_json) = NEW.worktree_history_event_count
      AND json_array_length(NEW.stage_history_json) = NEW.stage_history_event_count
      AND json_array_length(NEW.lease_history_json) = NEW.lease_history_event_count
      AND json_array_length(NEW.reservation_history_json) = NEW.reservation_history_event_count
      AND json_array_length(NEW.orchestration_history_json) =
        NEW.orchestration_history_event_count
      AND (SELECT count(*)
        FROM agent_control_implementation_result_evidence result
        JOIN agent_control_implementation_stage_finalization_receipts receipt
          ON receipt.result_evidence_id IS result.result_evidence_id
        JOIN agent_control_implementation_stage_finalization_markers marker
          ON marker.result_evidence_id IS result.result_evidence_id
        JOIN agent_control_events terminal_stage
          ON terminal_stage.event_id IS result.stage_event_id
        JOIN agent_control_events lease_release
          ON lease_release.event_id IS result.lease_event_id
        WHERE result.result_evidence_id IS NEW.implementation_result_evidence_id
          AND result.result_json IS NEW.implementation_result_json
          AND result.finalization_fingerprint IS NEW.implementation_finalization_fingerprint
          AND receipt.receipt_id IS NEW.implementation_finalization_receipt_id
          AND marker.marker_id IS NEW.implementation_finalization_marker_id
          AND receipt.status = 'accepted' AND result.outcome = 'succeeded'
          AND result.handoff_id IS NEW.handoff_id
          AND result.handoff_fingerprint IS NEW.handoff_fingerprint
          AND result.project_id IS NEW.project_id AND result.task_id IS NEW.task_id
          AND result.task_revision IS NEW.task_revision
          AND result.github_intake_sequence IS NEW.github_intake_sequence
          AND result.source_identity_fingerprint IS NEW.source_identity_fingerprint
          AND result.repository_display IS NEW.repository_display
          AND result.source_revision IS NEW.source_revision
          AND result.worktree_reservation_id IS NEW.worktree_reservation_id
          AND result.worktree_event_id IS NEW.worktree_event_id
          AND result.worktree_event_sequence IS NEW.worktree_event_sequence
          AND result.worktree_event_stream_version IS NEW.worktree_event_stream_version
          AND result.worktree_ownership_fingerprint IS NEW.worktree_ownership_fingerprint
          AND result.stage_run_id IS NEW.implementation_stage_run_id
          AND result.attempt_id IS NEW.implementation_attempt_id
          AND result.controlled_thread_reservation_id IS
            NEW.implementation_controlled_thread_reservation_id
          AND result.thread_id IS NEW.implementation_thread_id
          AND result.stage_event_id IS NEW.implementation_terminal_stage_event_id
          AND result.stage_event_sequence IS
            NEW.implementation_terminal_stage_event_sequence
          AND result.stage_event_stream_version IS
            NEW.implementation_terminal_stage_event_stream_version
          AND result.lease_id IS NEW.lease_id
          AND result.lease_holder_id IS NEW.lease_holder_id
          AND result.fence_token IS NEW.implementation_fence_token
          AND result.lease_event_id IS NEW.implementation_lease_release_event_id
          AND result.lease_event_sequence IS NEW.implementation_lease_release_event_sequence
          AND result.lease_event_stream_version IS
            NEW.implementation_lease_release_stream_version
          AND result.orchestration_history_json IS NEW.orchestration_history_json
          AND result.orchestration_history_digest IS NEW.orchestration_history_digest
          AND result.orchestration_history_event_count IS NEW.orchestration_history_event_count
          AND terminal_stage.event_type = 'agentControl.stageRun.implementationSucceeded'
          AND terminal_stage.stream_version = 3
          AND lease_release.event_type =
            'agentControl.stageRunLease.releasedAfterImplementation') = 1
      AND (SELECT count(*)
        FROM agent_control_events stage_event
        JOIN agent_control_events lease_event
          ON lease_event.event_id IS NEW.verification_lease_event_id
        JOIN agent_control_events reservation_event
          ON reservation_event.event_id IS NEW.verification_reservation_event_id
        JOIN agent_control_stage_run_states stage
          ON stage.stage_run_id IS NEW.verification_stage_run_id
        JOIN agent_control_stage_run_lease_states lease
          ON lease.lease_id IS NEW.lease_id
        JOIN agent_control_verification_thread_reservation_states reservation
          ON reservation.controlled_thread_reservation_id IS
            NEW.verification_controlled_thread_reservation_id
        WHERE stage_event.event_id IS NEW.verification_stage_event_id
          AND stage_event.sequence IS NEW.verification_stage_event_sequence
          AND stage_event.stream_id IS NEW.verification_stage_run_id
          AND stage_event.stream_version = 1
          AND stage_event.event_type = 'agentControl.stageRun.prepared'
          AND lease_event.sequence IS NEW.verification_lease_event_sequence
          AND lease_event.stream_id IS NEW.lease_id
          AND lease_event.stream_version IS NEW.verification_lease_event_stream_version
          AND lease_event.event_type = 'agentControl.stageRunLease.reserved'
          AND reservation_event.sequence IS NEW.verification_reservation_event_sequence
          AND reservation_event.stream_id IS
            NEW.verification_controlled_thread_reservation_id
          AND reservation_event.stream_version = 1
          AND reservation_event.event_type =
            'agentControl.controlledThreadReservation.prepared'
          AND stage.status = 'prepared' AND stage.revision = 1
          AND stage.role_id = 'verifier' AND stage.stage_kind = 'verification'
          AND stage.stage_ordinal = 3 AND stage.attempt_ordinal = 1
          AND stage.attempt_id IS NEW.verification_attempt_id
          AND lease.status = 'reserved' AND lease.stage_run_id IS NEW.verification_stage_run_id
          AND lease.attempt_id IS NEW.verification_attempt_id
          AND lease.holder_id IS NEW.lease_holder_id
          AND lease.fence_token IS NEW.verification_fence_token
          AND reservation.status = 'prepared' AND reservation.revision = 1
          AND reservation.stage_run_id IS NEW.verification_stage_run_id
          AND reservation.attempt_id IS NEW.verification_attempt_id
          AND reservation.thread_id IS NEW.verification_thread_id
          AND reservation.lease_id IS NEW.lease_id
          AND reservation.fence_token IS NEW.verification_fence_token
          AND reservation.worktree_reservation_id IS NEW.worktree_reservation_id) = 1
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'verification admission evidence is inconsistent'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_admission_receipt_validate
    BEFORE INSERT ON agent_control_verification_admission_receipts
    WHEN NOT COALESCE(((SELECT count(*)
      FROM agent_control_verification_admission_evidence evidence
      WHERE evidence.receipt_id IS NEW.receipt_id
        AND evidence.marker_id IS NEW.marker_id
        AND evidence.admission_command_id IS NEW.admission_command_id
        AND evidence.admission_fingerprint IS NEW.admission_fingerprint
        AND evidence.admission_evidence_id IS NEW.admission_evidence_id
        AND evidence.implementation_result_evidence_id IS
          NEW.implementation_result_evidence_id
        AND evidence.verification_stage_event_id IS NEW.verification_stage_event_id
        AND evidence.verification_stage_event_sequence IS
          NEW.verification_stage_event_sequence
        AND evidence.verification_lease_event_id IS NEW.verification_lease_event_id
        AND evidence.verification_lease_event_sequence IS
          NEW.verification_lease_event_sequence
        AND evidence.verification_reservation_event_id IS
          NEW.verification_reservation_event_id
        AND evidence.verification_reservation_event_sequence IS
          NEW.verification_reservation_event_sequence
        AND evidence.admitted_at IS NEW.accepted_at
        AND NEW.status = 'accepted') = 1), 0)
    BEGIN SELECT RAISE(ABORT, 'verification admission receipt is inconsistent'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_admission_marker_validate
    BEFORE INSERT ON agent_control_verification_admission_markers
    WHEN NOT COALESCE(((SELECT count(*)
      FROM agent_control_verification_admission_evidence evidence
      JOIN agent_control_verification_admission_receipts receipt
        ON receipt.admission_evidence_id IS evidence.admission_evidence_id
      WHERE evidence.marker_id IS NEW.marker_id
        AND evidence.receipt_id IS NEW.receipt_id
        AND receipt.receipt_id IS NEW.receipt_id
        AND evidence.admission_command_id IS NEW.admission_command_id
        AND receipt.admission_command_id IS NEW.admission_command_id
        AND evidence.admission_fingerprint IS NEW.admission_fingerprint
        AND receipt.admission_fingerprint IS NEW.admission_fingerprint
        AND evidence.admission_evidence_id IS NEW.admission_evidence_id
        AND evidence.implementation_result_evidence_id IS
          NEW.implementation_result_evidence_id
        AND receipt.implementation_result_evidence_id IS
          NEW.implementation_result_evidence_id
        AND evidence.admitted_at IS NEW.committed_at
        AND receipt.accepted_at IS NEW.committed_at
        AND receipt.status = 'accepted') = 1), 0)
    BEGIN SELECT RAISE(ABORT, 'verification admission marker is inconsistent'); END
  `).unprepared;

  for (const table of [
    "agent_control_verification_admission_evidence",
    "agent_control_verification_admission_receipts",
    "agent_control_verification_admission_markers",
  ] as const) {
    yield* sql.unsafe(`
      CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
      BEGIN SELECT RAISE(ABORT, 'verification admission evidence is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table}
      BEGIN SELECT RAISE(ABORT, 'verification admission evidence is immutable'); END
    `).unprepared;
  }
});

/** Durable admission of verification@3; materialization and execution remain closed. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const existing = yield* sql<{ readonly count: number }>`
    SELECT count(*) AS count FROM sqlite_schema
    WHERE type = 'table' AND name = 'agent_control_verification_admission_markers'
  `;
  if (existing[0]?.count === 1) return;

  yield* sql`PRAGMA defer_foreign_keys = ON`;
  yield* isolateExistingControlledThreadValidation;
  yield* createVerificationReservationBoundary;
  yield* createVerificationEventAndProjectionValidation;
  yield* createVerificationReservationValidation;
  yield* createAdmissionCompanions;
  yield* createAdmissionStorageValidation;
  yield* createAdmissionCrossValidation;

  const violations = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
  if (violations.length !== 0) {
    return yield* Effect.die(new Error("migration 057 introduced foreign-key violations"));
  }
  yield* sql`PRAGMA defer_foreign_keys = OFF`;
});
