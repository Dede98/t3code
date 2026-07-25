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
    CREATE UNIQUE INDEX idx_agent_control_events_worktree_relational_identity
    ON agent_control_events(event_id, stream_id, stream_version, event_type)
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
    CREATE TABLE agent_control_worktree_stream_catalog (
      reservation_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      stage_run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 1),
      created_at TEXT NOT NULL,
      initial_event_id TEXT NOT NULL UNIQUE,
      initial_stream_version INTEGER NOT NULL DEFAULT 1 CHECK (initial_stream_version = 1),
      UNIQUE (
        reservation_id, project_id, task_id, stage_run_id, attempt_id, lease_id, fence_token
      ),
      FOREIGN KEY (initial_event_id, reservation_id, initial_stream_version)
        REFERENCES agent_control_worktree_event_envelopes(
          event_id, reservation_id, stream_version
        )
        DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_worktree_catalog_project
    ON agent_control_worktree_stream_catalog(project_id, reservation_id)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_worktree_catalog_identity
    ON agent_control_worktree_stream_catalog(
      project_id, task_id, stage_run_id, attempt_id, lease_id, fence_token
    )
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_stream_catalog_immutable_update
    BEFORE UPDATE ON agent_control_worktree_stream_catalog
    BEGIN
      SELECT RAISE(ABORT, 'worktree stream catalog is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_stream_catalog_immutable_delete
    BEFORE DELETE ON agent_control_worktree_stream_catalog
    BEGIN
      SELECT RAISE(ABORT, 'worktree stream catalog is immutable');
    END
  `;

  yield* sql`
    CREATE TABLE agent_control_worktree_event_envelopes (
      event_id TEXT PRIMARY KEY,
      reservation_id TEXT NOT NULL,
      stream_version INTEGER NOT NULL CHECK (stream_version >= 1),
      event_type TEXT NOT NULL CHECK (event_type IN (
        'agentControl.worktree.reserved',
        'agentControl.worktree.materializationStarted',
        'agentControl.worktree.ready',
        'agentControl.worktree.needsAttention'
      )),
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      stage_run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 1),
      created_at TEXT NOT NULL,
      UNIQUE (reservation_id, stream_version),
      UNIQUE (event_id, reservation_id, stream_version),
      CHECK (
        (stream_version = 1 AND event_type = 'agentControl.worktree.reserved')
        OR
        (stream_version > 1 AND event_type <> 'agentControl.worktree.reserved')
      ),
      FOREIGN KEY (
        reservation_id, project_id, task_id, stage_run_id, attempt_id, lease_id, fence_token
      ) REFERENCES agent_control_worktree_stream_catalog(
        reservation_id, project_id, task_id, stage_run_id, attempt_id, lease_id, fence_token
      ) DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (event_id, reservation_id, stream_version, event_type)
        REFERENCES agent_control_events(event_id, stream_id, stream_version, event_type)
        DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_worktree_envelopes_identity
    ON agent_control_worktree_event_envelopes(
      reservation_id, project_id, task_id, stage_run_id, attempt_id, lease_id, fence_token
    )
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_event_envelope_catalog_identity_insert
    BEFORE INSERT ON agent_control_worktree_event_envelopes
    BEGIN
      SELECT CASE
        WHEN COALESCE((
          SELECT COUNT(*)
          FROM agent_control_worktree_stream_catalog AS catalog
          WHERE catalog.reservation_id = NEW.reservation_id
            AND catalog.project_id = NEW.project_id
            AND catalog.task_id = NEW.task_id
            AND catalog.stage_run_id = NEW.stage_run_id
            AND catalog.attempt_id = NEW.attempt_id
            AND catalog.lease_id = NEW.lease_id
            AND catalog.fence_token = NEW.fence_token
        ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree event envelope catalog identity mismatch')
      END;
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_event_envelope_immutable_update
    BEFORE UPDATE ON agent_control_worktree_event_envelopes
    BEGIN
      SELECT RAISE(ABORT, 'worktree event envelope is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_event_envelope_immutable_delete
    BEFORE DELETE ON agent_control_worktree_event_envelopes
    BEGIN
      SELECT RAISE(ABORT, 'worktree event envelope is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_event_identity_insert
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'worktree-reservation'
    BEGIN
      SELECT CASE
        WHEN COALESCE((
          SELECT COUNT(*)
          FROM agent_control_worktree_event_envelopes AS envelope
          JOIN agent_control_worktree_stream_catalog AS catalog
            ON catalog.reservation_id = envelope.reservation_id
           AND catalog.project_id = envelope.project_id
           AND catalog.task_id = envelope.task_id
           AND catalog.stage_run_id = envelope.stage_run_id
           AND catalog.attempt_id = envelope.attempt_id
           AND catalog.lease_id = envelope.lease_id
           AND catalog.fence_token = envelope.fence_token
          WHERE envelope.event_id = NEW.event_id
            AND envelope.reservation_id = NEW.stream_id
            AND envelope.stream_version = NEW.stream_version
            AND envelope.event_type = NEW.event_type
        ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree event envelope identity mismatch')
      END;
      SELECT CASE
        WHEN NEW.actor_authority = 'controller'
          AND (
            NEW.stream_version = 1
            OR COALESCE((
              SELECT COUNT(*)
              FROM agent_control_events AS previous
              WHERE previous.aggregate_kind = 'worktree-reservation'
                AND previous.stream_id = NEW.stream_id
                AND previous.stream_version = NEW.stream_version - 1
            ), 0) = 1
          )
        THEN 1
        ELSE RAISE(ABORT, 'worktree event stream continuity mismatch')
      END;
      SELECT CASE
        WHEN COALESCE(json_valid(NEW.payload_json), 0) = 1
          AND COALESCE(json_type(NEW.payload_json, '$') = 'object', 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree event payload must be a valid object')
      END;
      SELECT CASE
        WHEN COALESCE((
          SELECT CASE
            WHEN COUNT(*) = 1
              AND MAX(CASE
                WHEN value.type = 'text' AND value.atom = NEW.stream_id THEN 1 ELSE 0
              END) = 1
            THEN 1 ELSE 0
          END
          FROM json_each(NEW.payload_json) AS value
          WHERE value.key = 'reservationId'
        ), 0) = 1
          AND COALESCE((
            SELECT CASE
              WHEN COUNT(*) = 1
                AND MAX(CASE
                  WHEN value.type = 'text' AND value.atom = envelope.project_id
                  THEN 1 ELSE 0
                END) = 1
              THEN 1 ELSE 0
            END
            FROM json_each(NEW.payload_json) AS value
            JOIN agent_control_worktree_event_envelopes AS envelope
              ON envelope.event_id = NEW.event_id
            WHERE value.key = 'projectId'
          ), 0) = 1
          AND COALESCE((
            SELECT CASE
              WHEN COUNT(*) = 1
                AND MAX(CASE
                  WHEN value.type = 'text' AND value.atom = envelope.task_id
                  THEN 1 ELSE 0
                END) = 1
              THEN 1 ELSE 0
            END
            FROM json_each(NEW.payload_json) AS value
            JOIN agent_control_worktree_event_envelopes AS envelope
              ON envelope.event_id = NEW.event_id
            WHERE value.key = 'taskId'
          ), 0) = 1
          AND COALESCE((
            SELECT CASE
              WHEN COUNT(*) = 1
                AND MAX(CASE
                  WHEN value.type = 'text' AND value.atom = envelope.stage_run_id
                  THEN 1 ELSE 0
                END) = 1
              THEN 1 ELSE 0
            END
            FROM json_each(NEW.payload_json) AS value
            JOIN agent_control_worktree_event_envelopes AS envelope
              ON envelope.event_id = NEW.event_id
            WHERE value.key = 'stageRunId'
          ), 0) = 1
          AND COALESCE((
            SELECT CASE
              WHEN COUNT(*) = 1
                AND MAX(CASE
                  WHEN value.type = 'text' AND value.atom = envelope.attempt_id
                  THEN 1 ELSE 0
                END) = 1
              THEN 1 ELSE 0
            END
            FROM json_each(NEW.payload_json) AS value
            JOIN agent_control_worktree_event_envelopes AS envelope
              ON envelope.event_id = NEW.event_id
            WHERE value.key = 'attemptId'
          ), 0) = 1
          AND COALESCE((
            SELECT CASE
              WHEN COUNT(*) = 1
                AND MAX(CASE
                  WHEN value.type = 'text' AND value.atom = envelope.lease_id
                  THEN 1 ELSE 0
                END) = 1
              THEN 1 ELSE 0
            END
            FROM json_each(NEW.payload_json) AS value
            JOIN agent_control_worktree_event_envelopes AS envelope
              ON envelope.event_id = NEW.event_id
            WHERE value.key = 'leaseId'
          ), 0) = 1
          AND COALESCE((
            SELECT CASE
              WHEN COUNT(*) = 1
                AND MAX(CASE
                  WHEN value.type = 'integer' AND value.atom = envelope.fence_token
                  THEN 1 ELSE 0
                END) = 1
              THEN 1 ELSE 0
            END
            FROM json_each(NEW.payload_json) AS value
            JOIN agent_control_worktree_event_envelopes AS envelope
              ON envelope.event_id = NEW.event_id
            WHERE value.key = 'fenceToken'
          ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree event payload identity mismatch')
      END;
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_event_immutable_update
    BEFORE UPDATE ON agent_control_events
    WHEN OLD.aggregate_kind = 'worktree-reservation'
      OR NEW.aggregate_kind = 'worktree-reservation'
    BEGIN
      SELECT RAISE(ABORT, 'worktree event is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_event_payload_complete_insert
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'worktree-reservation'
    BEGIN
      SELECT CASE NEW.event_type
        WHEN 'agentControl.worktree.reserved' THEN
          CASE WHEN
            COALESCE((
              SELECT COUNT(*) = 23 AND COUNT(DISTINCT value.key) = 23
                AND MIN(CASE
                  WHEN value.key IN (
                    'reservationId', 'projectId', 'taskId', 'sourceIdentityFingerprint',
                    'stageRunId', 'attemptId', 'leaseId', 'repositoryWorkspace',
                    'repositoryCommonDir', 'baseRef', 'baseCommitSha', 'branchName',
                    'internalWorktreePath', 'targetGenerationId', 'reservedAt'
                  ) AND value.type = 'text' AND length(value.atom) > 0 THEN 1
                  WHEN value.key IN (
                    'taskRevision', 'githubIntakeSequence', 'fenceToken'
                  ) AND value.type = 'integer' AND value.atom >= 1 THEN 1
                  WHEN value.key IN (
                    'worktreeRootDevice', 'worktreeRootInode',
                    'worktreeParentDevice', 'worktreeParentInode'
                  ) AND value.type = 'integer' AND value.atom >= 0 THEN 1
                  WHEN value.key = 'repository' AND value.type = 'object' THEN 1
                  ELSE 0
                END) = 1
              FROM json_each(NEW.payload_json) AS value
            ), 0) = 1
            AND COALESCE((
              SELECT COUNT(*) = 8 AND COUNT(DISTINCT value.key) = 8
                AND MIN(CASE
                  WHEN value.key IN (
                    'repositoryNodeId', 'nameWithOwner', 'canonicalKey', 'remoteName',
                    'remoteUrl', 'defaultRemoteRef'
                  ) AND value.type = 'text' AND length(value.atom) > 0 THEN 1
                  WHEN value.key IN ('commonDirDevice', 'commonDirInode')
                    AND value.type = 'integer' AND value.atom >= 0 THEN 1
                  ELSE 0
                END) = 1
              FROM json_each(NEW.payload_json, '$.repository') AS value
            ), 0) = 1
          THEN 1 ELSE RAISE(ABORT, 'incomplete worktree reserved payload') END
        WHEN 'agentControl.worktree.materializationStarted' THEN
          CASE WHEN COALESCE((
            SELECT COUNT(*) = 8 AND COUNT(DISTINCT value.key) = 8
              AND MIN(CASE
                WHEN value.key IN (
                  'reservationId', 'projectId', 'taskId', 'stageRunId',
                  'attemptId', 'leaseId', 'transitionedAt'
                ) AND value.type = 'text' AND length(value.atom) > 0 THEN 1
                WHEN value.key = 'fenceToken'
                  AND value.type = 'integer' AND value.atom >= 1 THEN 1
                ELSE 0
              END) = 1
            FROM json_each(NEW.payload_json) AS value
          ), 0) = 1
          THEN 1 ELSE RAISE(ABORT, 'incomplete worktree materializing payload') END
        WHEN 'agentControl.worktree.ready' THEN
          CASE WHEN COALESCE((
            SELECT COUNT(*) = 15 AND COUNT(DISTINCT value.key) = 15
              AND MIN(CASE
                WHEN value.key IN (
                  'reservationId', 'projectId', 'taskId', 'stageRunId', 'attemptId',
                  'leaseId', 'transitionedAt', 'headCommitSha', 'ownershipFingerprint',
                  'gitCreatedGitDir', 'markedOwnershipFingerprint', 'verifiedAt'
                ) AND value.type = 'text' AND length(value.atom) > 0 THEN 1
                WHEN value.key = 'fenceToken'
                  AND value.type = 'integer' AND value.atom >= 1 THEN 1
                WHEN value.key IN ('gitCreatedDevice', 'gitCreatedInode')
                  AND value.type = 'integer' AND value.atom >= 0 THEN 1
                ELSE 0
              END) = 1
            FROM json_each(NEW.payload_json) AS value
          ), 0) = 1
          THEN 1 ELSE RAISE(ABORT, 'incomplete worktree ready payload') END
        WHEN 'agentControl.worktree.needsAttention' THEN
          CASE WHEN COALESCE((
            SELECT COUNT(*) = 14 AND COUNT(DISTINCT value.key) = 14
              AND MIN(CASE
                WHEN value.key IN (
                  'reservationId', 'projectId', 'taskId', 'stageRunId', 'attemptId',
                  'leaseId', 'transitionedAt', 'attentionCode', 'materializationPhase'
                ) AND value.type = 'text' AND length(value.atom) > 0 THEN 1
                WHEN value.key = 'fenceToken'
                  AND value.type = 'integer' AND value.atom >= 1 THEN 1
                WHEN value.key IN ('gitCreatedDevice', 'gitCreatedInode')
                  AND (
                    value.type = 'null'
                    OR (value.type = 'integer' AND value.atom >= 0)
                  ) THEN 1
                WHEN value.key IN ('gitCreatedGitDir', 'markedOwnershipFingerprint')
                  AND (
                    value.type = 'null'
                    OR (value.type = 'text' AND length(value.atom) > 0)
                  ) THEN 1
                ELSE 0
              END) = 1
            FROM json_each(NEW.payload_json) AS value
          ), 0) = 1
          THEN 1 ELSE RAISE(ABORT, 'incomplete worktree attention payload') END
        ELSE 0
      END = 1;
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_event_immutable_delete
    BEFORE DELETE ON agent_control_events
    WHEN OLD.aggregate_kind = 'worktree-reservation'
    BEGIN
      SELECT RAISE(ABORT, 'worktree event is immutable');
    END
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
      target_generation_id TEXT NOT NULL CHECK (
        length(target_generation_id) = 64
        AND target_generation_id NOT GLOB '*[^0-9a-f]*'
      ),
      worktree_root_device INTEGER NOT NULL CHECK (worktree_root_device >= 0),
      worktree_root_inode INTEGER NOT NULL CHECK (worktree_root_inode >= 0),
      worktree_parent_device INTEGER NOT NULL CHECK (worktree_parent_device >= 0),
      worktree_parent_inode INTEGER NOT NULL CHECK (worktree_parent_inode >= 0),
      materialization_phase TEXT NOT NULL CHECK (materialization_phase IN (
        'reserved', 'materializing', 'git-created', 'ownership-marked'
      )),
      git_created_device INTEGER CHECK (git_created_device IS NULL OR git_created_device >= 0),
      git_created_inode INTEGER CHECK (git_created_inode IS NULL OR git_created_inode >= 0),
      git_created_git_dir TEXT,
      marked_ownership_fingerprint TEXT CHECK (
        marked_ownership_fingerprint IS NULL
        OR (
          length(marked_ownership_fingerprint) = 64
          AND marked_ownership_fingerprint NOT GLOB '*[^0-9a-f]*'
        )
      ),
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
        (status = 'reserved' AND materialization_phase = 'reserved'
          AND git_created_device IS NULL AND git_created_inode IS NULL
          AND git_created_git_dir IS NULL AND marked_ownership_fingerprint IS NULL)
        OR
        (status = 'materializing' AND materialization_phase = 'materializing'
          AND git_created_device IS NULL AND git_created_inode IS NULL
          AND git_created_git_dir IS NULL AND marked_ownership_fingerprint IS NULL)
        OR
        (status = 'ready' AND materialization_phase = 'ownership-marked'
          AND git_created_device IS NOT NULL AND git_created_inode IS NOT NULL
          AND git_created_git_dir IS NOT NULL AND marked_ownership_fingerprint IS NOT NULL)
        OR
        (status = 'needs-attention' AND (
          (materialization_phase IN ('reserved', 'materializing')
            AND git_created_device IS NULL AND git_created_inode IS NULL
            AND git_created_git_dir IS NULL AND marked_ownership_fingerprint IS NULL)
          OR
          (materialization_phase = 'git-created'
            AND git_created_device IS NOT NULL AND git_created_inode IS NOT NULL
            AND git_created_git_dir IS NOT NULL AND marked_ownership_fingerprint IS NULL)
          OR
          (materialization_phase = 'ownership-marked'
            AND git_created_device IS NOT NULL AND git_created_inode IS NOT NULL
            AND git_created_git_dir IS NOT NULL AND marked_ownership_fingerprint IS NOT NULL)
        ))
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
      command_id TEXT PRIMARY KEY CHECK (
        length(command_id) > 0
        AND command_id NOT GLOB 'agent-control-internal-worktree-v1-*'
      ),
      command_type TEXT NOT NULL CHECK (command_type IN (
        'reserve-and-materialize', 'reconcile'
      )),
      input_fingerprint TEXT NOT NULL CHECK (
        length(input_fingerprint) = 64
        AND input_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      project_id TEXT NOT NULL,
      task_id TEXT,
      reservation_id TEXT,
      worktree_reservation_id TEXT,
      target_generation_id TEXT CHECK (
        target_generation_id IS NULL
        OR (
          length(target_generation_id) = 64
          AND target_generation_id NOT GLOB '*[^0-9a-f]*'
        )
      ),
      status TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'rejected')),
      pending_token TEXT,
      claim_runtime_id TEXT,
      claim_attempt_id TEXT,
      claim_started_at TEXT,
      materialization_phase TEXT NOT NULL DEFAULT 'unbound' CHECK (materialization_phase IN (
        'unbound', 'reserved', 'materializing', 'git-created',
        'ownership-marked', 'terminal'
      )),
      git_created_device INTEGER CHECK (
        git_created_device IS NULL OR git_created_device >= 0
      ),
      git_created_inode INTEGER CHECK (
        git_created_inode IS NULL OR git_created_inode >= 0
      ),
      git_created_git_dir TEXT,
      marked_ownership_fingerprint TEXT CHECK (
        marked_ownership_fingerprint IS NULL
        OR (
          length(marked_ownership_fingerprint) = 64
          AND marked_ownership_fingerprint NOT GLOB '*[^0-9a-f]*'
        )
      ),
      result_json TEXT,
      result_status TEXT CHECK (
        result_status IS NULL OR result_status IN ('ready', 'needs-attention')
      ),
      result_reservation_id TEXT,
      result_revision INTEGER CHECK (result_revision IS NULL OR result_revision >= 1),
      result_sequence INTEGER CHECK (result_sequence IS NULL OR result_sequence >= 1),
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
      revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1),
      CHECK (
        (command_type = 'reserve-and-materialize'
          AND task_id IS NOT NULL AND reservation_id IS NULL)
        OR
        (command_type = 'reconcile'
          AND task_id IS NULL AND reservation_id IS NOT NULL)
      ),
      CHECK (
        (pending_token IS NULL AND claim_runtime_id IS NULL
          AND claim_attempt_id IS NULL AND claim_started_at IS NULL)
        OR
        (status = 'pending' AND pending_token IS NOT NULL
          AND claim_runtime_id IS NOT NULL AND claim_attempt_id IS NOT NULL
          AND claim_started_at IS NOT NULL)
      ),
      CHECK (
        (materialization_phase = 'unbound'
          AND worktree_reservation_id IS NULL
          AND git_created_device IS NULL AND git_created_inode IS NULL
          AND git_created_git_dir IS NULL AND marked_ownership_fingerprint IS NULL)
        OR
        (materialization_phase IN ('reserved', 'materializing')
          AND worktree_reservation_id IS NOT NULL AND target_generation_id IS NOT NULL
          AND git_created_device IS NULL AND git_created_inode IS NULL
          AND git_created_git_dir IS NULL AND marked_ownership_fingerprint IS NULL)
        OR
        (materialization_phase = 'git-created'
          AND worktree_reservation_id IS NOT NULL AND target_generation_id IS NOT NULL
          AND git_created_device IS NOT NULL AND git_created_inode IS NOT NULL
          AND git_created_git_dir IS NOT NULL AND marked_ownership_fingerprint IS NULL)
        OR
        (materialization_phase = 'ownership-marked'
          AND worktree_reservation_id IS NOT NULL AND target_generation_id IS NOT NULL
          AND git_created_device IS NOT NULL AND git_created_inode IS NOT NULL
          AND git_created_git_dir IS NOT NULL AND marked_ownership_fingerprint IS NOT NULL)
        OR
        (materialization_phase = 'terminal'
          AND (
            (git_created_device IS NULL AND git_created_inode IS NULL
              AND git_created_git_dir IS NULL
              AND marked_ownership_fingerprint IS NULL)
            OR
            (worktree_reservation_id IS NOT NULL
              AND target_generation_id IS NOT NULL
              AND git_created_device IS NOT NULL AND git_created_inode IS NOT NULL
              AND git_created_git_dir IS NOT NULL
              AND marked_ownership_fingerprint IS NULL)
            OR
            (worktree_reservation_id IS NOT NULL
              AND target_generation_id IS NOT NULL
              AND git_created_device IS NOT NULL AND git_created_inode IS NOT NULL
              AND git_created_git_dir IS NOT NULL
              AND marked_ownership_fingerprint IS NOT NULL)
          ))
      ),
      CHECK (
        (status = 'pending' AND result_json IS NULL AND result_status IS NULL
          AND rejection_code IS NULL
          AND result_reservation_id IS NULL AND result_revision IS NULL
          AND result_sequence IS NULL AND completed_at IS NULL
          AND materialization_phase <> 'terminal')
        OR (status = 'accepted' AND result_json IS NOT NULL
          AND result_status IS NOT NULL
          AND result_reservation_id IS NOT NULL AND result_revision IS NOT NULL
          AND result_sequence IS NOT NULL AND rejection_code IS NULL
          AND completed_at IS NOT NULL AND pending_token IS NULL
          AND claim_runtime_id IS NULL AND claim_attempt_id IS NULL
          AND claim_started_at IS NULL AND materialization_phase = 'terminal'
          AND worktree_reservation_id = result_reservation_id
          AND COALESCE(json_valid(result_json), 0) = 1
          AND COALESCE(json_type(result_json, '$') = 'object', 0) = 1
          AND CASE
            WHEN json_type(result_json, '$.status') = 'text'
            THEN COALESCE(json_extract(result_json, '$.status') = result_status, 0)
            ELSE 0
          END = 1
          AND CASE
            WHEN json_type(result_json, '$.reservationId') = 'text'
            THEN COALESCE(
              json_extract(result_json, '$.reservationId') = result_reservation_id, 0
            )
            ELSE 0
          END = 1
          AND CASE
            WHEN json_type(result_json, '$.revision') = 'integer'
            THEN COALESCE(json_extract(result_json, '$.revision') = result_revision, 0)
            ELSE 0
          END = 1
          AND CASE
            WHEN json_type(result_json, '$.sequence') = 'integer'
            THEN COALESCE(json_extract(result_json, '$.sequence') = result_sequence, 0)
            ELSE 0
          END = 1
          AND CASE
            WHEN json_type(result_json, '$.projectId') = 'text'
            THEN COALESCE(json_extract(result_json, '$.projectId') = project_id, 0)
            ELSE 0
          END = 1
          AND CASE
            WHEN task_id IS NULL THEN 1
            WHEN json_type(result_json, '$.taskId') = 'text'
            THEN COALESCE(json_extract(result_json, '$.taskId') = task_id, 0)
            ELSE 0
          END = 1
          AND CASE
            WHEN json_type(result_json, '$.targetGenerationId') = 'text'
            THEN COALESCE(
              json_extract(result_json, '$.targetGenerationId') = target_generation_id, 0
            )
            ELSE 0
          END = 1
          AND CASE
            WHEN json_type(result_json, '$.gitCreatedDevice') = 'null'
            THEN git_created_device IS NULL
            WHEN json_type(result_json, '$.gitCreatedDevice') = 'integer'
            THEN COALESCE(
              json_extract(result_json, '$.gitCreatedDevice') = git_created_device, 0
            )
            ELSE 0
          END = 1
          AND CASE
            WHEN json_type(result_json, '$.gitCreatedInode') = 'null'
            THEN git_created_inode IS NULL
            WHEN json_type(result_json, '$.gitCreatedInode') = 'integer'
            THEN COALESCE(
              json_extract(result_json, '$.gitCreatedInode') = git_created_inode, 0
            )
            ELSE 0
          END = 1
          AND CASE
            WHEN json_type(result_json, '$.gitCreatedGitDir') = 'null'
            THEN git_created_git_dir IS NULL
            WHEN json_type(result_json, '$.gitCreatedGitDir') = 'text'
            THEN COALESCE(
              json_extract(result_json, '$.gitCreatedGitDir') = git_created_git_dir, 0
            )
            ELSE 0
          END = 1
          AND CASE
            WHEN json_type(result_json, '$.markedOwnershipFingerprint') = 'null'
            THEN marked_ownership_fingerprint IS NULL
            WHEN json_type(result_json, '$.markedOwnershipFingerprint') = 'text'
            THEN COALESCE(
              json_extract(result_json, '$.markedOwnershipFingerprint')
                = marked_ownership_fingerprint,
              0
            )
            ELSE 0
          END = 1
          AND (
            (result_status = 'needs-attention'
              AND (
                (git_created_device IS NULL AND git_created_inode IS NULL
                  AND git_created_git_dir IS NULL
                  AND marked_ownership_fingerprint IS NULL)
                OR
                (git_created_device IS NOT NULL AND git_created_inode IS NOT NULL
                  AND git_created_git_dir IS NOT NULL)
              ))
            OR
            (result_status = 'ready'
              AND git_created_device IS NOT NULL AND git_created_inode IS NOT NULL
              AND git_created_git_dir IS NOT NULL
              AND marked_ownership_fingerprint IS NOT NULL)
          ))
        OR (status = 'rejected' AND result_json IS NULL AND result_status IS NULL
          AND result_reservation_id IS NULL AND result_revision IS NULL
          AND result_sequence IS NULL AND rejection_code IS NOT NULL
          AND completed_at IS NOT NULL AND pending_token IS NULL
          AND claim_runtime_id IS NULL AND claim_attempt_id IS NULL
          AND claim_started_at IS NULL AND materialization_phase = 'terminal')
      )
    )
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_worktree_operations_reservation
    ON agent_control_worktree_controller_operations(worktree_reservation_id, status)
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_operation_result_json_insert
    BEFORE INSERT ON agent_control_worktree_controller_operations
    WHEN NEW.status = 'accepted'
    BEGIN
      SELECT CASE
        WHEN COALESCE(json_valid(NEW.result_json), 0) = 1
          AND COALESCE(json_type(NEW.result_json, '$') = 'object', 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree accepted result must be a valid object')
      END;
      SELECT CASE
        WHEN COALESCE((
          SELECT CASE
            WHEN COUNT(*) = 19
              AND COUNT(DISTINCT value.key) = 19
              AND MIN(CASE
                WHEN value.key IN (
                  'reservationId', 'projectId', 'taskId', 'status',
                  'targetGenerationId', 'internalWorktreePath',
                  'materializationPhase', 'leaseId', 'reservedAt', 'createdAt',
                  'updatedAt'
                ) AND value.type = 'text' THEN 1
                WHEN value.key IN ('revision', 'sequence', 'fenceToken')
                  AND value.type = 'integer' THEN 1
                WHEN value.key = 'repository' AND value.type = 'object' THEN 1
                WHEN value.key IN ('gitCreatedDevice', 'gitCreatedInode')
                  AND value.type IN ('integer', 'null') THEN 1
                WHEN value.key IN ('gitCreatedGitDir', 'markedOwnershipFingerprint')
                  AND value.type IN ('text', 'null') THEN 1
                ELSE 0
              END) = 1
            THEN 1 ELSE 0
          END
          FROM json_each(NEW.result_json) AS value
          WHERE value.key IN (
            'reservationId', 'projectId', 'taskId', 'status', 'revision',
            'sequence', 'targetGenerationId', 'internalWorktreePath',
            'repository', 'leaseId', 'fenceToken', 'materializationPhase',
            'gitCreatedDevice', 'gitCreatedInode', 'gitCreatedGitDir',
            'markedOwnershipFingerprint', 'reservedAt', 'createdAt', 'updatedAt'
          )
        ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree accepted result coordinate identity mismatch')
      END;
      SELECT CASE
        WHEN COALESCE((
          SELECT CASE
            WHEN COUNT(*) = 1
              AND MAX(CASE
                WHEN value.type = 'text' AND value.atom = NEW.result_status
                THEN 1 ELSE 0
              END) = 1
            THEN 1 ELSE 0
          END
          FROM json_each(NEW.result_json) AS value
          WHERE value.key = 'status'
        ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree accepted result status identity mismatch')
      END;
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_operation_result_json_update
    BEFORE UPDATE ON agent_control_worktree_controller_operations
    WHEN NEW.status = 'accepted'
    BEGIN
      SELECT CASE
        WHEN COALESCE(json_valid(NEW.result_json), 0) = 1
          AND COALESCE(json_type(NEW.result_json, '$') = 'object', 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree accepted result must be a valid object')
      END;
      SELECT CASE
        WHEN COALESCE((
          SELECT CASE
            WHEN COUNT(*) = 19
              AND COUNT(DISTINCT value.key) = 19
              AND MIN(CASE
                WHEN value.key IN (
                  'reservationId', 'projectId', 'taskId', 'status',
                  'targetGenerationId', 'internalWorktreePath',
                  'materializationPhase', 'leaseId', 'reservedAt', 'createdAt',
                  'updatedAt'
                ) AND value.type = 'text' THEN 1
                WHEN value.key IN ('revision', 'sequence', 'fenceToken')
                  AND value.type = 'integer' THEN 1
                WHEN value.key = 'repository' AND value.type = 'object' THEN 1
                WHEN value.key IN ('gitCreatedDevice', 'gitCreatedInode')
                  AND value.type IN ('integer', 'null') THEN 1
                WHEN value.key IN ('gitCreatedGitDir', 'markedOwnershipFingerprint')
                  AND value.type IN ('text', 'null') THEN 1
                ELSE 0
              END) = 1
            THEN 1 ELSE 0
          END
          FROM json_each(NEW.result_json) AS value
          WHERE value.key IN (
            'reservationId', 'projectId', 'taskId', 'status', 'revision',
            'sequence', 'targetGenerationId', 'internalWorktreePath',
            'repository', 'leaseId', 'fenceToken', 'materializationPhase',
            'gitCreatedDevice', 'gitCreatedInode', 'gitCreatedGitDir',
            'markedOwnershipFingerprint', 'reservedAt', 'createdAt', 'updatedAt'
          )
        ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree accepted result coordinate identity mismatch')
      END;
      SELECT CASE
        WHEN COALESCE((
          SELECT CASE
            WHEN COUNT(*) = 1
              AND MAX(CASE
                WHEN value.type = 'text' AND value.atom = NEW.result_status
                THEN 1 ELSE 0
              END) = 1
            THEN 1 ELSE 0
          END
          FROM json_each(NEW.result_json) AS value
          WHERE value.key = 'status'
        ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree accepted result status identity mismatch')
      END;
    END
  `;

  yield* sql`
    CREATE TABLE agent_control_worktree_target_claims (
      command_id TEXT PRIMARY KEY,
      input_fingerprint TEXT NOT NULL CHECK (
        length(input_fingerprint) = 64
        AND input_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      pending_token TEXT NOT NULL,
      claim_attempt_id TEXT NOT NULL,
      target_generation TEXT NOT NULL CHECK (
        length(target_generation) = 64
        AND target_generation NOT GLOB '*[^0-9a-f]*'
      ),
      reservation_id TEXT NOT NULL,
      target_path TEXT NOT NULL,
      parent_path TEXT NOT NULL,
      parent_device INTEGER NOT NULL CHECK (parent_device >= 0),
      parent_inode INTEGER NOT NULL CHECK (parent_inode >= 0),
      target_device INTEGER CHECK (target_device IS NULL OR target_device >= 0),
      target_inode INTEGER CHECK (target_inode IS NULL OR target_inode >= 0),
      target_uid INTEGER CHECK (target_uid IS NULL OR target_uid >= 0),
      target_mode INTEGER CHECK (target_mode IS NULL OR target_mode >= 0),
      phase TEXT NOT NULL CHECK (phase IN (
        'prepared', 'acquired', 'released', 'materialized', 'retained-attention'
      )),
      closed_git_device INTEGER CHECK (
        closed_git_device IS NULL OR closed_git_device >= 0
      ),
      closed_git_inode INTEGER CHECK (
        closed_git_inode IS NULL OR closed_git_inode >= 0
      ),
      closed_git_dir TEXT,
      closed_ownership_fingerprint TEXT CHECK (
        closed_ownership_fingerprint IS NULL
        OR (
          length(closed_ownership_fingerprint) = 64
          AND closed_ownership_fingerprint NOT GLOB '*[^0-9a-f]*'
        )
      ),
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1),
      CHECK (
        (phase = 'prepared' AND target_device IS NULL AND target_inode IS NULL
          AND target_uid IS NULL AND target_mode IS NULL
          AND closed_git_device IS NULL AND closed_git_inode IS NULL
          AND closed_git_dir IS NULL AND closed_ownership_fingerprint IS NULL)
        OR
        (phase = 'acquired' AND target_device IS NOT NULL AND target_inode IS NOT NULL
          AND target_uid IS NOT NULL AND target_mode IS NOT NULL
          AND closed_git_device IS NULL AND closed_git_inode IS NULL
          AND closed_git_dir IS NULL AND closed_ownership_fingerprint IS NULL)
        OR
        (phase = 'released'
          AND closed_git_device IS NULL AND closed_git_inode IS NULL
          AND closed_git_dir IS NULL AND closed_ownership_fingerprint IS NULL)
        OR
        (phase IN ('materialized', 'retained-attention')
          AND target_device IS NOT NULL AND target_inode IS NOT NULL
          AND target_uid IS NOT NULL AND target_mode IS NOT NULL
          AND closed_git_device IS NOT NULL AND closed_git_inode IS NOT NULL
          AND closed_git_dir IS NOT NULL)
      )
    )
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_worktree_target_claim_reservation
    ON agent_control_worktree_target_claims(reservation_id, target_path)
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_worktree_active_target_claim
    ON agent_control_worktree_target_claims(target_path)
    WHERE phase IN ('prepared', 'acquired')
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_target_claim_authority_insert
    BEFORE INSERT ON agent_control_worktree_target_claims
    BEGIN
      SELECT CASE
        WHEN COALESCE((
          SELECT COUNT(*)
          FROM agent_control_worktree_controller_operations AS operation
          JOIN agent_control_worktree_reservation_states AS reservation
            ON reservation.reservation_id = operation.worktree_reservation_id
          WHERE operation.command_id = NEW.command_id
            AND operation.input_fingerprint = NEW.input_fingerprint
            AND operation.pending_token = NEW.pending_token
            AND operation.claim_attempt_id = NEW.claim_attempt_id
            AND operation.status = 'pending'
            AND operation.worktree_reservation_id = NEW.reservation_id
            AND operation.target_generation_id = NEW.target_generation
            AND reservation.internal_worktree_path = NEW.target_path
            AND reservation.worktree_parent_device = NEW.parent_device
            AND reservation.worktree_parent_inode = NEW.parent_inode
        ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree target claim authority mismatch')
      END;
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_target_claim_authority_update
    BEFORE UPDATE ON agent_control_worktree_target_claims
    BEGIN
      SELECT CASE
        WHEN OLD.command_id = NEW.command_id
          AND OLD.input_fingerprint = NEW.input_fingerprint
          AND OLD.target_generation = NEW.target_generation
          AND OLD.reservation_id = NEW.reservation_id
          AND OLD.target_path = NEW.target_path
          AND OLD.parent_path = NEW.parent_path
          AND OLD.parent_device = NEW.parent_device
          AND OLD.parent_inode = NEW.parent_inode
          AND OLD.created_at = NEW.created_at
          AND (
            (OLD.phase = 'prepared'
              AND NEW.phase IN ('prepared', 'acquired', 'released'))
            OR
            (OLD.phase = 'acquired'
              AND NEW.phase IN (
                'prepared', 'acquired', 'released', 'materialized', 'retained-attention'
              ))
            OR
            (OLD.phase = 'released' AND NEW.phase = 'prepared')
          )
          AND COALESCE((
            SELECT COUNT(*)
            FROM agent_control_worktree_controller_operations AS operation
            WHERE operation.command_id = NEW.command_id
              AND operation.input_fingerprint = NEW.input_fingerprint
              AND operation.pending_token = NEW.pending_token
              AND operation.claim_attempt_id = NEW.claim_attempt_id
              AND operation.status = 'pending'
              AND operation.worktree_reservation_id = NEW.reservation_id
              AND operation.target_generation_id = NEW.target_generation
          ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'worktree target claim update authority mismatch')
      END;
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_worktree_terminal_operation_target_guard
    BEFORE UPDATE ON agent_control_worktree_controller_operations
    WHEN NEW.status IN ('accepted', 'rejected')
    BEGIN
      SELECT CASE
        WHEN COALESCE((
          SELECT COUNT(*)
          FROM agent_control_worktree_target_claims AS claim
          WHERE claim.command_id = NEW.command_id
            AND claim.input_fingerprint = NEW.input_fingerprint
            AND claim.reservation_id = NEW.worktree_reservation_id
            AND claim.target_generation = NEW.target_generation_id
            AND claim.phase IN ('prepared', 'acquired')
        ), 0) = 0
        THEN 1
        ELSE RAISE(ABORT, 'terminal worktree operation has active target claim')
      END;
      SELECT CASE
        WHEN NEW.status <> 'accepted' OR NEW.result_status <> 'ready'
          OR COALESCE((
            SELECT COUNT(*)
            FROM agent_control_worktree_target_claims AS claim
            WHERE claim.command_id = NEW.command_id
              AND claim.input_fingerprint = NEW.input_fingerprint
              AND claim.reservation_id = NEW.result_reservation_id
              AND claim.target_generation = NEW.target_generation_id
              AND claim.phase = 'materialized'
              AND claim.closed_git_device = NEW.git_created_device
              AND claim.closed_git_inode = NEW.git_created_inode
              AND claim.closed_git_dir = NEW.git_created_git_dir
          ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'ready worktree operation lacks materialized target claim')
      END;
      SELECT CASE
        WHEN NEW.status <> 'accepted'
          OR NEW.result_status <> 'needs-attention'
          OR NEW.git_created_device IS NULL
          OR COALESCE((
            SELECT COUNT(*)
            FROM agent_control_worktree_target_claims AS claim
            WHERE claim.command_id = NEW.command_id
              AND claim.input_fingerprint = NEW.input_fingerprint
              AND claim.reservation_id = NEW.result_reservation_id
              AND claim.target_generation = NEW.target_generation_id
              AND claim.phase = 'retained-attention'
              AND claim.closed_git_device = NEW.git_created_device
              AND claim.closed_git_inode = NEW.git_created_inode
              AND claim.closed_git_dir = NEW.git_created_git_dir
              AND claim.closed_ownership_fingerprint
                IS NEW.marked_ownership_fingerprint
          ), 0) = 1
        THEN 1
        ELSE RAISE(ABORT, 'attention worktree operation lacks retained target evidence')
      END;
    END
  `;
});
