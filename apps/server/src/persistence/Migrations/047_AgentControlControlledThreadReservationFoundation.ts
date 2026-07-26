import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const quoteSqliteIdentifier = (identifier: string): string =>
  `"${identifier.replaceAll('"', '""')}"`;

/**
 * Adds the isolated Controlled Thread reservation aggregate. Existing rows,
 * receipt codes, worktree integrity triggers, and AUTOINCREMENT coordinates are
 * retained byte-for-byte. No projection foreign keys are introduced.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const dependentTriggers = yield* sql<{
    readonly name: string;
    readonly sql: string | null;
  }>`
    SELECT name, sql
    FROM sqlite_schema
    WHERE type = 'trigger'
      AND (
        sql LIKE '%agent_control_events%'
        OR sql LIKE '%agent_control_command_receipts%'
      )
      AND sql IS NOT NULL
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
    CREATE TABLE agent_control_events_rebuild_047 (
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
          AND event_type = 'agentControl.controlledThreadReservation.prepared'
          AND stream_version = 1
          AND actor_authority = 'controller'
        )
      )
    )
  `;
  yield* sql`
    INSERT INTO agent_control_events_rebuild_047 (
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
  yield* sql`ALTER TABLE agent_control_events_rebuild_047 RENAME TO agent_control_events`;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_stream_version
    ON agent_control_events(aggregate_kind, stream_id, stream_version)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_events_stream_sequence
    ON agent_control_events(aggregate_kind, stream_id, sequence)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_events_command_id ON agent_control_events(command_id)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_events_correlation_id ON agent_control_events(correlation_id)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_events_sequence ON agent_control_events(sequence)
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_worktree_relational_identity
    ON agent_control_events(event_id, stream_id, stream_version, event_type)
  `;
  if (eventSequence !== undefined) {
    yield* sql`
      DELETE FROM sqlite_sequence
      WHERE name IN ('agent_control_events', 'agent_control_events_rebuild_047')
    `;
    yield* sql`
      INSERT INTO sqlite_sequence(name, seq)
      VALUES ('agent_control_events', ${eventSequence})
    `;
  }

  yield* sql`
    CREATE TABLE agent_control_command_receipts_rebuild_047 (
      command_id TEXT PRIMARY KEY,
      command_fingerprint TEXT NOT NULL,
      authority TEXT NOT NULL CHECK (authority IN ('human', 'controller', 'system')),
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind IN (
        'project-controller', 'github-intake', 'task', 'stage-run',
        'stage-run-lease', 'worktree-reservation',
        'controlled-thread-reservation'
      )),
      aggregate_id TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('accepted', 'rejected')),
      result_sequence INTEGER NOT NULL CHECK (result_sequence >= 0),
      result_stream_version INTEGER NOT NULL CHECK (result_stream_version >= 0),
      event_created INTEGER NOT NULL CHECK (event_created IN (0, 1)),
      accepted_at TEXT NOT NULL,
      error_code TEXT,
      CHECK (
        COALESCE(
          (status = 'accepted' AND error_code IS NULL)
          OR (status = 'rejected' AND error_code IN (
          'validation', 'project-missing', 'project-deleted', 'revision-conflict',
          'transition-not-allowed', 'mode-not-available', 'tracker-not-configured',
          'repository-not-github', 'repository-identity-conflict', 'poll-in-progress',
          'github-unavailable', 'github-authentication', 'github-timeout',
          'github-command-failed', 'github-decode-failed', 'pagination-overflow',
          'timeline-incomplete', 'repository-identity-changed', 'issue-repository-changed',
          'task-missing', 'source-identity-conflict', 'source-state-conflict',
          'source-snapshot-stale', 'task-projection-corrupt', 'project-unavailable',
          'project-mode-inactive', 'task-not-candidate', 'task-ineligible',
          'task-stage-inactive', 'source-watermark-stale', 'stage-run-missing',
          'stage-run-identity-conflict', 'stage-run-projection-corrupt',
          'stage-run-not-prepared', 'stage-run-history-ambiguous', 'lease-missing',
          'lease-already-reserved', 'lease-projection-corrupt', 'holder-mismatch',
          'fence-token-mismatch', 'lease-not-reserved', 'lease-expired',
          'lease-foreign-runtime', 'lease-recovery-required', 'reservation-missing',
          'reservation-conflict', 'reservation-projection-corrupt',
          'repository-unavailable', 'default-remote-ref-unavailable',
          'repository-identity-mismatch', 'branch-name-invalid',
          'worktree-path-invalid', 'state-not-available', 'repository-lock-unavailable',
          'source-snapshot-unavailable', 'command-identity-mismatch',
          'command-previously-rejected', 'internal-persistence-error',
          'worktree-missing', 'worktree-not-ready', 'worktree-projection-corrupt',
          'worktree-history-ambiguous', 'controlled-thread-reservation-missing',
          'controlled-thread-reservation-identity-conflict',
          'controlled-thread-reservation-corrupt'
          )),
          0
        ) = 1
      )
    )
  `;
  yield* sql`
    INSERT INTO agent_control_command_receipts_rebuild_047 (
      command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
      status, result_sequence, result_stream_version, event_created,
      accepted_at, error_code
    )
    SELECT command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
      status, result_sequence, result_stream_version, event_created,
      accepted_at, error_code
    FROM agent_control_command_receipts
  `;
  yield* sql`DROP TABLE agent_control_command_receipts`;
  yield* sql`
    ALTER TABLE agent_control_command_receipts_rebuild_047
    RENAME TO agent_control_command_receipts
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_receipts_aggregate
    ON agent_control_command_receipts(aggregate_kind, aggregate_id)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_receipts_sequence
    ON agent_control_command_receipts(result_sequence)
  `;
  for (const trigger of dependentTriggers) {
    if (trigger.sql !== null) yield* sql.unsafe(trigger.sql).unprepared;
  }

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
      status TEXT NOT NULL CHECK (status = 'prepared'),
      revision INTEGER NOT NULL CHECK (revision = 1),
      last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 1),
      prepared_at TEXT NOT NULL,
      state_json TEXT NOT NULL CHECK (
        COALESCE(
          json_valid(state_json) = 1 AND json_type(state_json) = 'object',
          0
        ) = 1
      )
    )
  `;
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
      controlled_thread_reservation_id TEXT PRIMARY KEY,
      event_id TEXT NOT NULL UNIQUE,
      stream_version INTEGER NOT NULL CHECK (stream_version = 1),
      command_id TEXT NOT NULL,
      event_type TEXT NOT NULL CHECK (
        event_type = 'agentControl.controlledThreadReservation.prepared'
      ),
      thread_id TEXT NOT NULL CHECK (
        thread_id LIKE 't3-auto-reserved-thread-%'
      ),
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
      FOREIGN KEY (event_id) REFERENCES agent_control_events(event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_controlled_thread_catalog_position
    ON agent_control_controlled_thread_stream_catalog(
      project_id, task_id, stage_run_id, attempt_id,
      task_revision, github_intake_sequence, source_identity_fingerprint
    )
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
        NEW.stream_version = 1
        AND NEW.event_type = 'agentControl.controlledThreadReservation.prepared'
        AND NEW.actor_authority = 'controller'
        AND NEW.causation_event_id IS NULL
        AND NEW.correlation_id = NEW.command_id
        AND json_valid(NEW.payload_json) = 1
        AND json_type(NEW.payload_json) = 'object'
        AND json_valid(NEW.metadata_json) = 1
        AND json_type(NEW.metadata_json) = 'object'
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
        AND (SELECT count(*) FROM json_each(NEW.metadata_json)) = 1
        AND (SELECT count(DISTINCT key) FROM json_each(NEW.metadata_json)) = 1
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
        AND json_extract(NEW.payload_json, '$.status') = 'prepared'
        AND json_type(NEW.payload_json, '$.preparedAt') = 'text'
        AND length(json_extract(NEW.payload_json, '$.preparedAt')) = 24
        AND json_extract(NEW.payload_json, '$.preparedAt') GLOB
          '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
        AND EXISTS (
          SELECT 1
          FROM agent_control_controlled_thread_stream_catalog AS catalog
          WHERE catalog.controlled_thread_reservation_id = NEW.stream_id
            AND catalog.event_id = NEW.event_id
            AND catalog.stream_version = NEW.stream_version
            AND catalog.command_id = NEW.command_id
            AND catalog.event_type = NEW.event_type
            AND catalog.controlled_thread_reservation_id =
              json_extract(NEW.payload_json, '$.controlledThreadReservationId')
            AND catalog.thread_id = json_extract(NEW.payload_json, '$.threadId')
            AND catalog.project_id = json_extract(NEW.payload_json, '$.projectId')
            AND catalog.task_id = json_extract(NEW.payload_json, '$.taskId')
            AND catalog.task_revision = json_extract(NEW.payload_json, '$.taskRevision')
            AND catalog.github_intake_sequence =
              json_extract(NEW.payload_json, '$.githubIntakeSequence')
            AND catalog.source_identity_fingerprint =
              json_extract(NEW.payload_json, '$.sourceIdentityFingerprint')
            AND catalog.stage_run_id = json_extract(NEW.payload_json, '$.stageRunId')
            AND catalog.attempt_id = json_extract(NEW.payload_json, '$.attemptId')
            AND catalog.role_id = json_extract(NEW.payload_json, '$.roleId')
            AND catalog.stage_kind = json_extract(NEW.payload_json, '$.stageKind')
            AND catalog.stage_ordinal = json_extract(NEW.payload_json, '$.stageOrdinal')
            AND catalog.attempt_ordinal = json_extract(NEW.payload_json, '$.attemptOrdinal')
            AND catalog.lease_id = json_extract(NEW.payload_json, '$.leaseId')
            AND catalog.fence_token = json_extract(NEW.payload_json, '$.fenceToken')
            AND catalog.worktree_reservation_id =
              json_extract(NEW.payload_json, '$.worktreeReservationId')
            AND catalog.prepared_at = json_extract(NEW.payload_json, '$.preparedAt')
            AND NEW.stream_id =
              json_extract(NEW.payload_json, '$.controlledThreadReservationId')
            AND NEW.occurred_at = json_extract(NEW.payload_json, '$.preparedAt')
        )
      ), 0) <> 1
      THEN RAISE(ABORT, 'invalid controlled thread reservation event') END;
    END
  `;
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
        AND json_type(NEW.state_json, '$.schemaVersion') = 'integer'
        AND json_extract(NEW.state_json, '$.schemaVersion') = 1
        AND json_type(NEW.state_json, '$.revision') = 'integer'
        AND json_extract(NEW.state_json, '$.revision') = 1
        AND json_type(NEW.state_json, '$.sequence') = 'integer'
        AND json_extract(NEW.state_json, '$.sequence') >= 1
        AND json_type(NEW.state_json, '$.status') = 'text'
        AND json_extract(NEW.state_json, '$.status') = 'prepared'
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
        AND EXISTS (
          SELECT 1
          FROM agent_control_controlled_thread_stream_catalog AS catalog
          JOIN agent_control_events AS event
            ON event.event_id = catalog.event_id
          WHERE catalog.controlled_thread_reservation_id =
            NEW.controlled_thread_reservation_id
            AND catalog.thread_id = NEW.thread_id
            AND catalog.project_id = NEW.project_id
            AND catalog.task_id = NEW.task_id
            AND catalog.task_revision = NEW.task_revision
            AND catalog.github_intake_sequence = NEW.github_intake_sequence
            AND catalog.source_identity_fingerprint = NEW.source_identity_fingerprint
            AND catalog.stage_run_id = NEW.stage_run_id
            AND catalog.attempt_id = NEW.attempt_id
            AND catalog.role_id = NEW.role_id
            AND catalog.stage_kind = NEW.stage_kind
            AND catalog.stage_ordinal = NEW.stage_ordinal
            AND catalog.attempt_ordinal = NEW.attempt_ordinal
            AND catalog.lease_id = NEW.lease_id
            AND catalog.fence_token = NEW.fence_token
            AND catalog.worktree_reservation_id = NEW.worktree_reservation_id
            AND catalog.prepared_at = NEW.prepared_at
            AND event.sequence = NEW.last_event_sequence
            AND event.stream_version = NEW.revision
        )
      ), 0) <> 1
      THEN RAISE(ABORT, 'invalid controlled thread reservation projection') END;
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_projection_validate_update
    BEFORE UPDATE ON agent_control_controlled_thread_reservation_states
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread reservation projections are immutable');
    END
  `;

  yield* sql`
    CREATE TABLE agent_control_controlled_thread_command_intents (
      command_id TEXT PRIMARY KEY,
      request_fingerprint TEXT NOT NULL CHECK (
        length(request_fingerprint) = 64
        AND request_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      intent_fingerprint TEXT NOT NULL CHECK (
        length(intent_fingerprint) = 64
        AND intent_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      command_type TEXT NOT NULL CHECK (command_type IN (
        'agentControl.controlledThreadReservation.prepareInitial',
        'agentControl.controlledThreadReservation.prepare',
        'agentControl.controlledThreadReservation.transition'
      )),
      authority TEXT NOT NULL CHECK (authority = 'controller'),
      aggregate_kind TEXT NOT NULL CHECK (
        aggregate_kind = 'controlled-thread-reservation'
      ),
      aggregate_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      controlled_thread_reservation_id TEXT,
      thread_id TEXT,
      task_revision INTEGER,
      github_intake_sequence INTEGER,
      source_identity_fingerprint TEXT,
      stage_run_id TEXT,
      attempt_id TEXT,
      role_id TEXT,
      stage_kind TEXT,
      stage_ordinal INTEGER,
      attempt_ordinal INTEGER,
      lease_id TEXT,
      fence_token INTEGER,
      worktree_reservation_id TEXT,
      expected_revision INTEGER,
      target_status TEXT,
      CHECK (
        COALESCE(
          (
            command_type = 'agentControl.controlledThreadReservation.prepareInitial'
            AND controlled_thread_reservation_id IS NULL
            AND thread_id IS NULL
            AND task_revision IS NULL
            AND github_intake_sequence IS NULL
            AND source_identity_fingerprint IS NULL
            AND stage_run_id IS NULL
            AND attempt_id IS NULL
            AND role_id IS NULL
            AND stage_kind IS NULL
            AND stage_ordinal IS NULL
            AND attempt_ordinal IS NULL
            AND lease_id IS NULL
            AND fence_token IS NULL
            AND worktree_reservation_id IS NULL
            AND expected_revision IS NULL
            AND target_status IS NULL
          )
          OR
          (
            command_type IN (
              'agentControl.controlledThreadReservation.prepare',
              'agentControl.controlledThreadReservation.transition'
            )
            AND controlled_thread_reservation_id IS NOT NULL
            AND aggregate_id = controlled_thread_reservation_id
            AND thread_id IS NOT NULL
            AND thread_id LIKE 't3-auto-reserved-thread-%'
            AND task_revision >= 1
            AND github_intake_sequence >= 1
            AND length(source_identity_fingerprint) = 64
            AND source_identity_fingerprint NOT GLOB '*[^0-9a-f]*'
            AND stage_run_id IS NOT NULL
            AND attempt_id IS NOT NULL
            AND role_id = 'planning'
            AND stage_kind = 'planning'
            AND stage_ordinal = 1
            AND attempt_ordinal = 1
            AND lease_id IS NOT NULL
            AND fence_token >= 1
            AND worktree_reservation_id IS NOT NULL
            AND expected_revision = 0
            AND (
              (command_type = 'agentControl.controlledThreadReservation.prepare'
                AND target_status IS NULL)
              OR
              (command_type = 'agentControl.controlledThreadReservation.transition'
                AND target_status IN ('materializing', 'bound', 'released', 'invalidated'))
            )
          )
        , 0) = 1
      ),
      FOREIGN KEY (command_id) REFERENCES agent_control_command_receipts(command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_intent_no_update
    BEFORE UPDATE ON agent_control_controlled_thread_command_intents
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread command intent is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_intent_no_delete
    BEFORE DELETE ON agent_control_controlled_thread_command_intents
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread command intent is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_receipt_validate
    BEFORE INSERT ON agent_control_command_receipts
    WHEN NEW.aggregate_kind = 'controlled-thread-reservation'
    BEGIN
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM agent_control_controlled_thread_command_intents AS intent
        WHERE intent.command_id = NEW.command_id
          AND intent.request_fingerprint = NEW.command_fingerprint
          AND intent.authority = NEW.authority
          AND intent.aggregate_kind = NEW.aggregate_kind
          AND intent.aggregate_id = NEW.aggregate_id
      ) THEN RAISE(ABORT, 'missing controlled thread command intent') END;
      SELECT CASE WHEN NEW.status = 'accepted' AND NOT EXISTS (
          SELECT 1
          FROM agent_control_controlled_thread_command_intents AS intent
          JOIN agent_control_controlled_thread_stream_catalog AS catalog
            ON catalog.controlled_thread_reservation_id = intent.aggregate_id
          JOIN agent_control_events AS event
            ON event.event_id = catalog.event_id
          WHERE intent.command_id = NEW.command_id
            AND intent.command_type =
              'agentControl.controlledThreadReservation.prepare'
            AND catalog.command_id = NEW.command_id
            AND event.sequence = NEW.result_sequence
            AND event.stream_version = NEW.result_stream_version
            AND NEW.event_created = 1
            AND NEW.result_sequence >= 1
            AND NEW.result_stream_version >= 1
            AND NEW.error_code IS NULL
      )
      THEN RAISE(ABORT, 'invalid accepted controlled thread receipt') END;
      SELECT CASE WHEN NEW.status = 'rejected' AND NOT EXISTS (
        SELECT 1
        WHERE NEW.event_created = 0
          AND NEW.result_sequence = 0
          AND NEW.result_stream_version = 0
          AND NEW.error_code IN (
          'validation', 'project-unavailable', 'project-mode-inactive',
          'task-missing', 'task-not-candidate', 'task-ineligible',
          'task-stage-inactive', 'source-snapshot-stale',
          'source-watermark-stale', 'stage-run-missing',
          'stage-run-not-prepared', 'stage-run-history-ambiguous',
          'lease-missing', 'lease-not-reserved', 'lease-expired',
          'lease-foreign-runtime', 'fence-token-mismatch',
          'worktree-missing', 'worktree-not-ready',
          'controlled-thread-reservation-missing',
          'controlled-thread-reservation-identity-conflict',
          'revision-conflict', 'state-not-available',
          'command-identity-mismatch', 'command-previously-rejected'
          )
      )
      THEN RAISE(ABORT, 'invalid rejected controlled thread receipt') END;
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_receipt_no_update
    BEFORE UPDATE ON agent_control_command_receipts
    WHEN OLD.aggregate_kind = 'controlled-thread-reservation'
      OR NEW.aggregate_kind = 'controlled-thread-reservation'
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread receipts are immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_receipt_no_delete
    BEFORE DELETE ON agent_control_command_receipts
    WHEN OLD.aggregate_kind = 'controlled-thread-reservation'
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread receipts are immutable');
    END
  `;
});
