import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** Adds the isolated, reconstructible Agent Control worktree reservation aggregate. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE agent_control_events_rebuild_046 (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind IN (
        'project-controller', 'github-intake', 'task', 'stage-run',
        'stage-run-lease', 'worktree-reservation'
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
      )
    )
  `;
  yield* sql`
    INSERT INTO agent_control_events_rebuild_046 (
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
  yield* sql`ALTER TABLE agent_control_events_rebuild_046 RENAME TO agent_control_events`;
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
    CREATE TABLE agent_control_command_receipts_rebuild_046 (
      command_id TEXT PRIMARY KEY,
      command_fingerprint TEXT NOT NULL,
      authority TEXT NOT NULL CHECK (authority IN ('human', 'controller', 'system')),
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind IN (
        'project-controller', 'github-intake', 'task', 'stage-run',
        'stage-run-lease', 'worktree-reservation'
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
          'repository-identity-mismatch',
          'branch-name-invalid', 'worktree-path-invalid', 'state-not-available',
          'repository-lock-unavailable',
          'source-snapshot-unavailable', 'command-identity-mismatch',
          'command-previously-rejected',
          'internal-persistence-error'
        ))
      )
    )
  `;
  yield* sql`
    INSERT INTO agent_control_command_receipts_rebuild_046 (
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
    ALTER TABLE agent_control_command_receipts_rebuild_046
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
    CREATE TABLE agent_control_worktree_reservation_states (
      reservation_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL,
      stage_run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 1),
      repository_node_id TEXT NOT NULL,
      repository_name_with_owner TEXT NOT NULL,
      repository_canonical_key TEXT NOT NULL,
      repository_remote_name TEXT NOT NULL,
      repository_remote_url TEXT NOT NULL,
      repository_default_remote_ref TEXT NOT NULL,
      repository_common_dir_device INTEGER NOT NULL CHECK (repository_common_dir_device >= 0),
      repository_common_dir_inode INTEGER NOT NULL CHECK (repository_common_dir_inode >= 0),
      repository_workspace TEXT NOT NULL,
      repository_common_dir TEXT NOT NULL,
      base_ref TEXT NOT NULL,
      base_commit_sha TEXT NOT NULL CHECK (
        length(base_commit_sha) IN (40, 64)
        AND base_commit_sha NOT GLOB '*[^0-9a-f]*'
      ),
      branch_name TEXT NOT NULL,
      internal_worktree_path TEXT NOT NULL,
      worktree_root_device INTEGER NOT NULL CHECK (worktree_root_device >= 0),
      worktree_root_inode INTEGER NOT NULL CHECK (worktree_root_inode >= 0),
      worktree_parent_device INTEGER NOT NULL CHECK (worktree_parent_device >= 0),
      worktree_parent_inode INTEGER NOT NULL CHECK (worktree_parent_inode >= 0),
      head_commit_sha TEXT,
      ownership_fingerprint TEXT,
      verified_at TEXT,
      status TEXT NOT NULL CHECK (
        status IN ('reserved', 'materializing', 'ready', 'needs-attention')
      ),
      attention_code TEXT CHECK (attention_code IS NULL OR attention_code IN (
        'path-occupied', 'branch-commit-mismatch', 'branch-in-other-worktree',
        'worktree-registration-mismatch', 'worktree-registration-ambiguous',
        'worktree-branch-mismatch',
        'worktree-head-mismatch', 'repository-identity-mismatch',
        'ownership-unproven', 'ownership-mismatch', 'worktree-dirty',
        'worktree-sequencer-state'
      )),
      state_json TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK (revision >= 1),
      last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 1),
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      CHECK (
        (status = 'needs-attention' AND attention_code IS NOT NULL)
        OR (status <> 'needs-attention' AND attention_code IS NULL)
      ),
      CHECK (
        (status = 'ready' AND head_commit_sha IS NOT NULL
          AND ownership_fingerprint IS NOT NULL AND verified_at IS NOT NULL)
        OR (status <> 'ready' AND head_commit_sha IS NULL
          AND ownership_fingerprint IS NULL AND verified_at IS NULL)
      )
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_worktree_task_stage
    ON agent_control_worktree_reservation_states(
      project_id, task_id, stage_run_id, attempt_id
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_worktree_branch
    ON agent_control_worktree_reservation_states(repository_canonical_key, branch_name)
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_worktree_path
    ON agent_control_worktree_reservation_states(internal_worktree_path)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_worktree_project
    ON agent_control_worktree_reservation_states(project_id, status, reservation_id)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_worktree_sequence
    ON agent_control_worktree_reservation_states(last_event_sequence)
  `;

  yield* sql`
    CREATE TABLE agent_control_worktree_controller_operations (
      command_id TEXT PRIMARY KEY,
      command_type TEXT NOT NULL CHECK (command_type IN (
        'reserve-and-materialize', 'reconcile'
      )),
      input_fingerprint TEXT NOT NULL,
      project_id TEXT NOT NULL,
      task_id TEXT,
      reservation_id TEXT,
      worktree_reservation_id TEXT,
      status TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'rejected')),
      result_json TEXT,
      rejection_code TEXT CHECK (rejection_code IS NULL OR rejection_code IN (
        'validation', 'project-unavailable', 'project-mode-inactive', 'task-missing',
        'task-not-candidate', 'task-ineligible', 'task-stage-inactive',
        'task-projection-corrupt', 'source-snapshot-unavailable',
        'source-snapshot-stale', 'source-watermark-stale', 'stage-run-missing',
        'stage-run-not-prepared', 'stage-run-projection-corrupt',
        'stage-run-history-ambiguous', 'lease-missing', 'lease-not-reserved',
        'lease-expired', 'lease-foreign-runtime', 'lease-recovery-required',
        'lease-projection-corrupt', 'fence-token-mismatch', 'reservation-missing',
        'reservation-conflict', 'reservation-projection-corrupt',
        'revision-conflict', 'state-not-available', 'command-identity-mismatch',
        'command-previously-rejected', 'repository-unavailable',
        'repository-identity-mismatch', 'default-remote-ref-unavailable',
        'branch-name-invalid', 'worktree-path-invalid',
        'repository-lock-unavailable', 'internal-persistence-error'
      )),
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      completed_at TEXT,
      CHECK (
        (status = 'pending' AND result_json IS NULL AND rejection_code IS NULL
          AND completed_at IS NULL)
        OR (status = 'accepted' AND result_json IS NOT NULL
          AND rejection_code IS NULL AND completed_at IS NOT NULL)
        OR (status = 'rejected' AND result_json IS NULL
          AND rejection_code IS NOT NULL AND completed_at IS NOT NULL)
      )
    )
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_worktree_operations_reservation
    ON agent_control_worktree_controller_operations(worktree_reservation_id, status)
  `;
});
