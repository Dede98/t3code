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
        ))
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
      state_json TEXT NOT NULL CHECK (json_valid(state_json) = 1)
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
});
