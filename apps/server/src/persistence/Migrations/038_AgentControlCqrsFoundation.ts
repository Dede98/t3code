import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Agent Control owns a dedicated event store instead of extending the manual
 * orchestration tables. Its lifecycle, command authority, rebuild/recovery
 * boundary, and future task/run/GitHub aggregates are independent; none of
 * those concerns should widen the manual orchestration protocol.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS agent_control_events (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind = 'project-controller'),
      stream_id TEXT NOT NULL,
      stream_version INTEGER NOT NULL CHECK (stream_version >= 1),
      event_type TEXT NOT NULL CHECK (event_type = 'agentControl.project.mode.changed'),
      occurred_at TEXT NOT NULL,
      command_id TEXT NOT NULL,
      causation_event_id TEXT,
      correlation_id TEXT NOT NULL,
      actor_authority TEXT NOT NULL CHECK (actor_authority IN ('human', 'controller', 'system')),
      payload_json TEXT NOT NULL,
      metadata_json TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_control_events_stream_version
    ON agent_control_events(aggregate_kind, stream_id, stream_version)
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_agent_control_events_stream_sequence
    ON agent_control_events(aggregate_kind, stream_id, sequence)
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_agent_control_events_command_id
    ON agent_control_events(command_id)
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_agent_control_events_correlation_id
    ON agent_control_events(correlation_id)
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_agent_control_events_sequence
    ON agent_control_events(sequence)
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS agent_control_command_receipts (
      command_id TEXT PRIMARY KEY,
      command_fingerprint TEXT NOT NULL,
      authority TEXT NOT NULL CHECK (authority IN ('human', 'controller', 'system')),
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind = 'project-controller'),
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
          'mode-not-available'
        ))
      )
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_agent_control_receipts_aggregate
    ON agent_control_command_receipts(aggregate_kind, aggregate_id)
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_agent_control_receipts_sequence
    ON agent_control_command_receipts(result_sequence)
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS agent_control_project_states (
      project_id TEXT PRIMARY KEY,
      mode TEXT NOT NULL CHECK (mode IN ('manual', 'observe', 'paused')),
      paused_from_mode TEXT CHECK (paused_from_mode IS NULL OR paused_from_mode = 'observe'),
      revision INTEGER NOT NULL CHECK (revision >= 1),
      last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 1),
      updated_at TEXT NOT NULL,
      CHECK (
        (mode = 'paused' AND paused_from_mode = 'observe')
        OR
        (mode != 'paused' AND paused_from_mode IS NULL)
      )
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_agent_control_project_states_sequence
    ON agent_control_project_states(last_event_sequence)
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS agent_control_projection_state (
      projector_name TEXT PRIMARY KEY,
      last_applied_sequence INTEGER NOT NULL CHECK (last_applied_sequence >= 0),
      updated_at TEXT NOT NULL
    )
  `;
});
