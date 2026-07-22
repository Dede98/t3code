import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Adds the per-project github-intake aggregate to the Agent Control CQRS store.
 * Existing project-controller events and receipts are copied byte-for-byte;
 * the new mutable tables are disposable projections rebuilt from those events.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE agent_control_events_rebuild_039 (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL CHECK (
        aggregate_kind IN ('project-controller', 'github-intake')
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
      )
    )
  `;
  yield* sql`
    INSERT INTO agent_control_events_rebuild_039 (
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
  yield* sql`
    ALTER TABLE agent_control_events_rebuild_039
    RENAME TO agent_control_events
  `;
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
    CREATE TABLE agent_control_command_receipts_rebuild_039 (
      command_id TEXT PRIMARY KEY,
      command_fingerprint TEXT NOT NULL,
      authority TEXT NOT NULL CHECK (authority IN ('human', 'controller', 'system')),
      aggregate_kind TEXT NOT NULL CHECK (
        aggregate_kind IN ('project-controller', 'github-intake')
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
          'internal-persistence-error'
        ))
      )
    )
  `;
  yield* sql`
    INSERT INTO agent_control_command_receipts_rebuild_039 (
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
    ALTER TABLE agent_control_command_receipts_rebuild_039
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
    CREATE TABLE agent_control_github_intake_states (
      project_id TEXT PRIMARY KEY,
      state_json TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK (revision >= 1),
      last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 1),
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_github_states_sequence
    ON agent_control_github_intake_states(last_event_sequence)
  `;

  yield* sql`
    CREATE TABLE agent_control_github_issues (
      project_id TEXT NOT NULL,
      issue_node_id TEXT NOT NULL,
      issue_number INTEGER NOT NULL CHECK (issue_number >= 1),
      repository_node_id TEXT NOT NULL,
      snapshot_json TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (project_id, issue_node_id)
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_github_issues_number
    ON agent_control_github_issues(project_id, issue_number)
  `;

  yield* sql`
    CREATE TABLE agent_control_github_timeline_events (
      project_id TEXT NOT NULL,
      issue_node_id TEXT NOT NULL,
      external_event_id TEXT NOT NULL,
      event_type TEXT NOT NULL CHECK (event_type IN ('labeled', 'unlabeled', 'unknown')),
      label_name TEXT NOT NULL,
      actor_login TEXT,
      occurred_at TEXT NOT NULL,
      PRIMARY KEY (project_id, issue_node_id, external_event_id)
    )
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_github_timeline_order
    ON agent_control_github_timeline_events(project_id, issue_node_id, occurred_at, external_event_id)
  `;
});
