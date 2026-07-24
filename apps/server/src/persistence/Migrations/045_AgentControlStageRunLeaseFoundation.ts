import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Adds restart-safe writer reservations for prepared stage runs. The lease
 * projection is reconstructible and deliberately has no foreign keys.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE agent_control_events_rebuild_045 (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL CHECK (
        aggregate_kind IN (
          'project-controller', 'github-intake', 'task', 'stage-run', 'stage-run-lease'
        )
      ),
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
        OR
        (aggregate_kind = 'github-intake' AND event_type IN (
          'agentControl.github.config.set',
          'agentControl.github.config.cleared',
          'agentControl.github.poll.succeeded',
          'agentControl.github.poll.failed'
        ))
        OR
        (aggregate_kind = 'task' AND event_type IN (
          'agentControl.task.created',
          'agentControl.task.sourceGate.changed',
          'agentControl.task.needsAttentionMarked',
          'agentControl.task.sourceMissingRecovered'
        ))
        OR
        (aggregate_kind = 'stage-run' AND event_type = 'agentControl.stageRun.prepared')
        OR
        (aggregate_kind = 'stage-run-lease' AND event_type IN (
          'agentControl.stageRunLease.reserved',
          'agentControl.stageRunLease.renewed',
          'agentControl.stageRunLease.releasedBeforeExecution'
        ))
      )
    )
  `;
  yield* sql`
    INSERT INTO agent_control_events_rebuild_045 (
      sequence, event_id, aggregate_kind, stream_id, stream_version,
      event_type, occurred_at, command_id, causation_event_id,
      correlation_id, actor_authority, payload_json, metadata_json
    )
    SELECT
      sequence, event_id, aggregate_kind, stream_id, stream_version,
      event_type, occurred_at, command_id, causation_event_id,
      correlation_id, actor_authority, payload_json, metadata_json
    FROM agent_control_events
  `;
  yield* sql`DROP TABLE agent_control_events`;
  yield* sql`ALTER TABLE agent_control_events_rebuild_045 RENAME TO agent_control_events`;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_stream_version
    ON agent_control_events(aggregate_kind, stream_id, stream_version)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_events_stream_sequence
    ON agent_control_events(aggregate_kind, stream_id, sequence)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_events_command_id
    ON agent_control_events(command_id)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_events_correlation_id
    ON agent_control_events(correlation_id)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_events_sequence
    ON agent_control_events(sequence)
  `;

  yield* sql`
    CREATE TABLE agent_control_command_receipts_rebuild_045 (
      command_id TEXT PRIMARY KEY,
      command_fingerprint TEXT NOT NULL,
      authority TEXT NOT NULL CHECK (authority IN ('human', 'controller', 'system')),
      aggregate_kind TEXT NOT NULL CHECK (
        aggregate_kind IN (
          'project-controller', 'github-intake', 'task', 'stage-run', 'stage-run-lease'
        )
      ),
      aggregate_id TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('accepted', 'rejected')),
      result_sequence INTEGER NOT NULL CHECK (result_sequence >= 0),
      result_stream_version INTEGER NOT NULL CHECK (result_stream_version >= 0),
      event_created INTEGER NOT NULL CHECK (event_created IN (0, 1)),
      accepted_at TEXT NOT NULL,
      error_code TEXT,
      CHECK (
        (status = 'accepted' AND error_code IS NULL)
        OR
        (status = 'rejected' AND error_code IN (
          'validation',
          'project-missing',
          'project-deleted',
          'revision-conflict',
          'transition-not-allowed',
          'mode-not-available',
          'tracker-not-configured',
          'repository-not-github',
          'repository-identity-conflict',
          'poll-in-progress',
          'github-unavailable',
          'github-authentication',
          'github-timeout',
          'github-command-failed',
          'github-decode-failed',
          'pagination-overflow',
          'timeline-incomplete',
          'repository-identity-changed',
          'issue-repository-changed',
          'task-missing',
          'source-identity-conflict',
          'source-state-conflict',
          'source-snapshot-stale',
          'task-projection-corrupt',
          'project-unavailable',
          'project-mode-inactive',
          'task-not-candidate',
          'task-ineligible',
          'task-stage-inactive',
          'source-watermark-stale',
          'stage-run-missing',
          'stage-run-identity-conflict',
          'stage-run-projection-corrupt',
          'stage-run-not-prepared',
          'stage-run-history-ambiguous',
          'lease-missing',
          'lease-already-reserved',
          'lease-projection-corrupt',
          'holder-mismatch',
          'fence-token-mismatch',
          'state-not-available',
          'source-snapshot-unavailable',
          'internal-persistence-error'
        ))
      )
    )
  `;
  yield* sql`
    INSERT INTO agent_control_command_receipts_rebuild_045 (
      command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
      status, result_sequence, result_stream_version, event_created,
      accepted_at, error_code
    )
    SELECT
      command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
      status, result_sequence, result_stream_version, event_created,
      accepted_at, error_code
    FROM agent_control_command_receipts
  `;
  yield* sql`DROP TABLE agent_control_command_receipts`;
  yield* sql`
    ALTER TABLE agent_control_command_receipts_rebuild_045
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

  yield* sql`
    CREATE TABLE agent_control_stage_run_lease_states (
      lease_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      stage_run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        length(source_identity_fingerprint) = 64
        AND source_identity_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      holder_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 1),
      status TEXT NOT NULL CHECK (status IN ('reserved', 'released')),
      acquired_at TEXT NOT NULL,
      renewed_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      released_at TEXT,
      state_json TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK (revision >= 1),
      last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 1),
      CHECK (
        (status = 'reserved' AND released_at IS NULL)
        OR (status = 'released' AND released_at IS NOT NULL)
      )
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_stage_run_lease_scope
    ON agent_control_stage_run_lease_states(project_id, task_id)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_stage_run_lease_project
    ON agent_control_stage_run_lease_states(project_id, status, lease_id)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_stage_run_lease_task
    ON agent_control_stage_run_lease_states(project_id, task_id, lease_id)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_stage_run_lease_holder
    ON agent_control_stage_run_lease_states(holder_id, status)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_stage_run_lease_status_expiry
    ON agent_control_stage_run_lease_states(status, expires_at)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_stage_run_lease_sequence
    ON agent_control_stage_run_lease_states(last_event_sequence)
  `;
});
