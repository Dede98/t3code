import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Adds immutable evidence for the server-internal controlled-thread
 * materialization primitive. This migration is additive: orchestration events,
 * receipts, projections, indexes, and AUTOINCREMENT coordinates are untouched.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_orchestration_events_materialization_coordinates
    ON orchestration_events(
      event_id, command_id, aggregate_kind, stream_id, stream_version,
      event_type, sequence, occurred_at
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_orchestration_receipts_materialization_coordinates
    ON orchestration_command_receipts(
      command_id, authority, aggregate_kind, aggregate_id,
      accepted_at, result_sequence, status
    )
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS orchestration_agent_control_thread_materialization_intents (
      command_id TEXT PRIMARY KEY,
      command_type TEXT NOT NULL CHECK (
        command_type = 'thread.agent-control.materialize'
      ),
      authority TEXT NOT NULL CHECK (authority = 'agent-control'),
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind = 'thread'),
      command_fingerprint TEXT NOT NULL CHECK (
        length(command_fingerprint) = 64
        AND command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      controlled_thread_reservation_id TEXT NOT NULL,
      thread_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        length(trim(source_identity_fingerprint)) > 0
      ),
      stage_run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      role_id TEXT NOT NULL CHECK (length(trim(role_id)) > 0),
      stage_kind TEXT NOT NULL,
      stage_ordinal INTEGER NOT NULL CHECK (stage_ordinal >= 1),
      attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal >= 1),
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 1),
      worktree_reservation_id TEXT NOT NULL,
      title TEXT NOT NULL CHECK (length(trim(title)) > 0),
      model_selection_json TEXT NOT NULL CHECK (
        COALESCE(
          json_valid(model_selection_json) = 1
          AND json_type(model_selection_json) = 'object',
          0
        ) = 1
      ),
      runtime_mode TEXT NOT NULL CHECK (
        runtime_mode IN ('approval-required', 'auto-accept-edits', 'full-access')
      ),
      interaction_mode TEXT NOT NULL CHECK (interaction_mode IN ('default', 'plan')),
      branch TEXT NOT NULL CHECK (length(trim(branch)) > 0),
      worktree_path TEXT NOT NULL CHECK (length(trim(worktree_path)) > 0),
      binding_json TEXT NOT NULL CHECK (
        COALESCE(
          json_valid(binding_json) = 1
          AND json_type(binding_json) = 'object',
          0
        ) = 1
      ),
      created_event_id TEXT,
      created_event_type TEXT,
      created_event_sequence INTEGER,
      created_event_stream_version INTEGER,
      binding_event_id TEXT,
      binding_event_type TEXT,
      binding_event_sequence INTEGER,
      binding_event_stream_version INTEGER,
      accepted_receipt_command_id TEXT,
      receipt_status TEXT NOT NULL CHECK (receipt_status IN ('accepted', 'rejected')),
      receipt_result_sequence INTEGER NOT NULL CHECK (receipt_result_sequence >= 0),
      receipt_accepted_at TEXT NOT NULL,
      receipt_error TEXT,
      created_at TEXT NOT NULL,
      CHECK (
        COALESCE(
          (
            receipt_status = 'accepted'
            AND receipt_error IS NULL
            AND role_id = 'planning'
            AND stage_kind = 'planning'
            AND stage_ordinal = 1
            AND attempt_ordinal = 1
            AND length(source_identity_fingerprint) = 64
            AND source_identity_fingerprint NOT GLOB '*[^0-9a-f]*'
            AND runtime_mode = 'approval-required'
            AND interaction_mode = 'plan'
            AND json_extract(binding_json, '$.taskId') = task_id
            AND json_extract(binding_json, '$.stageRunId') = stage_run_id
            AND json_extract(binding_json, '$.attemptId') = attempt_id
            AND json_extract(binding_json, '$.roleId') = role_id
            AND json_extract(binding_json, '$.controlState') = 'controlled'
            AND created_event_id IS NOT NULL
            AND created_event_type = 'thread.created'
            AND created_event_sequence >= 1
            AND created_event_stream_version = 1
            AND binding_event_id IS NOT NULL
            AND binding_event_type = 'thread.agent-control-bound'
            AND binding_event_sequence = created_event_sequence + 1
            AND binding_event_stream_version = 2
            AND accepted_receipt_command_id = command_id
            AND receipt_result_sequence = binding_event_sequence
            AND receipt_accepted_at = created_at
          )
          OR
          (
            receipt_status = 'rejected'
            AND receipt_error IS NOT NULL
            AND created_event_id IS NULL
            AND created_event_type IS NULL
            AND created_event_sequence IS NULL
            AND created_event_stream_version IS NULL
            AND binding_event_id IS NULL
            AND binding_event_type IS NULL
            AND binding_event_sequence IS NULL
            AND binding_event_stream_version IS NULL
            AND accepted_receipt_command_id IS NULL
            AND receipt_accepted_at = created_at
          ),
          0
        ) = 1
      ),
      FOREIGN KEY (
        command_id, authority, aggregate_kind, thread_id,
        receipt_accepted_at, receipt_result_sequence, receipt_status
      ) REFERENCES orchestration_command_receipts (
        command_id, authority, aggregate_kind, aggregate_id,
        accepted_at, result_sequence, status
      ),
      FOREIGN KEY (
        created_event_id, command_id, aggregate_kind, thread_id,
        created_event_stream_version, created_event_type,
        created_event_sequence, created_at
      ) REFERENCES orchestration_events (
        event_id, command_id, aggregate_kind, stream_id,
        stream_version, event_type, sequence, occurred_at
      ),
      FOREIGN KEY (
        binding_event_id, command_id, aggregate_kind, thread_id,
        binding_event_stream_version, binding_event_type,
        binding_event_sequence, created_at
      ) REFERENCES orchestration_events (
        event_id, command_id, aggregate_kind, stream_id,
        stream_version, event_type, sequence, occurred_at
      ),
      FOREIGN KEY (
        accepted_receipt_command_id, command_type, authority, aggregate_kind,
        thread_id, command_fingerprint, receipt_result_sequence,
        receipt_accepted_at, receipt_status
      ) REFERENCES orchestration_agent_control_thread_materialization_receipts (
        command_id, command_type, authority, aggregate_kind, thread_id,
        command_fingerprint, result_sequence, accepted_at, status
      ) DEFERRABLE INITIALLY DEFERRED
    )
  `;

  yield* sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_orchestration_materialization_intent_receipt_evidence
    ON orchestration_agent_control_thread_materialization_intents(
      command_id, command_type, authority, aggregate_kind, thread_id,
      command_fingerprint, receipt_result_sequence, receipt_accepted_at,
      receipt_status
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS orchestration_agent_control_thread_materialization_receipts (
      command_id TEXT PRIMARY KEY,
      command_type TEXT NOT NULL CHECK (
        command_type = 'thread.agent-control.materialize'
      ),
      authority TEXT NOT NULL CHECK (authority = 'agent-control'),
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind = 'thread'),
      thread_id TEXT NOT NULL,
      command_fingerprint TEXT NOT NULL CHECK (
        length(command_fingerprint) = 64
        AND command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      result_sequence INTEGER NOT NULL CHECK (result_sequence >= 1),
      accepted_at TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status = 'accepted'),
      UNIQUE (
        command_id, command_type, authority, aggregate_kind, thread_id,
        command_fingerprint, result_sequence, accepted_at, status
      ),
      CHECK (length(trim(thread_id)) > 0),
      FOREIGN KEY (
        command_id, authority, aggregate_kind, thread_id,
        accepted_at, result_sequence, status
      ) REFERENCES orchestration_command_receipts (
        command_id, authority, aggregate_kind, aggregate_id,
        accepted_at, result_sequence, status
      ) DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (
        command_id, command_type, authority, aggregate_kind, thread_id,
        command_fingerprint, result_sequence, accepted_at, status
      ) REFERENCES orchestration_agent_control_thread_materialization_intents (
        command_id, command_type, authority, aggregate_kind, thread_id,
        command_fingerprint, receipt_result_sequence, receipt_accepted_at,
        receipt_status
      ) DEFERRABLE INITIALLY DEFERRED
    )
  `;

  yield* sql`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_orchestration_materialization_thread
    ON orchestration_agent_control_thread_materialization_intents(thread_id)
    WHERE receipt_status = 'accepted'
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_orchestration_materialization_reservation
    ON orchestration_agent_control_thread_materialization_intents(
      controlled_thread_reservation_id, receipt_status
    )
  `;
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS trg_orchestration_materialization_intent_immutable_update
    BEFORE UPDATE ON orchestration_agent_control_thread_materialization_intents
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread materialization intent is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_materialization_intent_immutable_delete
    BEFORE DELETE ON orchestration_agent_control_thread_materialization_intents
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread materialization intent is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_materialization_receipt_evidence_immutable_update
    BEFORE UPDATE ON orchestration_agent_control_thread_materialization_receipts
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread materialization receipt evidence is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_materialization_receipt_evidence_immutable_delete
    BEFORE DELETE ON orchestration_agent_control_thread_materialization_receipts
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread materialization receipt evidence is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_materialization_receipt_immutable_update
    BEFORE UPDATE ON orchestration_command_receipts
    WHEN EXISTS (
      SELECT 1
      FROM orchestration_agent_control_thread_materialization_receipts evidence
      WHERE evidence.command_id = OLD.command_id
    )
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread materialization receipt is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_materialization_receipt_immutable_delete
    BEFORE DELETE ON orchestration_command_receipts
    WHEN EXISTS (
      SELECT 1
      FROM orchestration_agent_control_thread_materialization_receipts evidence
      WHERE evidence.command_id = OLD.command_id
    )
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread materialization receipt is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_materialization_event_immutable_update
    BEFORE UPDATE ON orchestration_events
    WHEN EXISTS (
      SELECT 1
      FROM orchestration_agent_control_thread_materialization_intents intent
      WHERE intent.receipt_status = 'accepted'
        AND (
          intent.created_event_id = OLD.event_id
          OR intent.binding_event_id = OLD.event_id
        )
    )
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread materialization event is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_materialization_event_immutable_delete
    BEFORE DELETE ON orchestration_events
    WHEN EXISTS (
      SELECT 1
      FROM orchestration_agent_control_thread_materialization_intents intent
      WHERE intent.receipt_status = 'accepted'
        AND (
          intent.created_event_id = OLD.event_id
          OR intent.binding_event_id = OLD.event_id
        )
    )
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread materialization event is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_rejected_materialization_event_insert
    BEFORE INSERT ON orchestration_events
    WHEN NEW.command_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM orchestration_agent_control_thread_materialization_intents intent
        WHERE intent.command_id = NEW.command_id
          AND intent.receipt_status = 'rejected'
      )
    BEGIN
      SELECT RAISE(ABORT, 'rejected controlled thread materialization has an event');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_rejected_materialization_event_update
    BEFORE UPDATE ON orchestration_events
    WHEN NEW.command_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM orchestration_agent_control_thread_materialization_intents intent
        WHERE intent.command_id = NEW.command_id
          AND intent.receipt_status = 'rejected'
      )
    BEGIN
      SELECT RAISE(ABORT, 'rejected controlled thread materialization has an event');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_rejected_materialization_intent_has_no_event
    BEFORE INSERT ON orchestration_agent_control_thread_materialization_intents
    WHEN NEW.receipt_status = 'rejected'
      AND EXISTS (
        SELECT 1
        FROM orchestration_events event
        WHERE event.command_id = NEW.command_id
      )
    BEGIN
      SELECT RAISE(ABORT, 'rejected controlled thread materialization has an event');
    END
  `;
});
