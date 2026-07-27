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
    CREATE TRIGGER trg_orchestration_materialization_accepted_evidence_complete
    BEFORE INSERT ON orchestration_agent_control_thread_materialization_receipts
    WHEN NOT EXISTS (
      SELECT 1
      FROM orchestration_agent_control_thread_materialization_intents intent
      INNER JOIN orchestration_command_receipts receipt
        ON receipt.command_id IS intent.command_id
       AND receipt.authority IS intent.authority
       AND receipt.aggregate_kind IS intent.aggregate_kind
       AND receipt.aggregate_id IS intent.thread_id
       AND receipt.accepted_at IS intent.receipt_accepted_at
       AND receipt.result_sequence IS intent.receipt_result_sequence
       AND receipt.status IS intent.receipt_status
       AND receipt.error IS NULL
      INNER JOIN orchestration_events created
        ON created.event_id IS intent.created_event_id
       AND created.command_id IS intent.command_id
       AND created.aggregate_kind IS intent.aggregate_kind
       AND created.stream_id IS intent.thread_id
       AND created.stream_version IS intent.created_event_stream_version
       AND created.event_type IS intent.created_event_type
       AND created.sequence IS intent.created_event_sequence
       AND created.occurred_at IS intent.created_at
      INNER JOIN orchestration_events binding
        ON binding.event_id IS intent.binding_event_id
       AND binding.command_id IS intent.command_id
       AND binding.aggregate_kind IS intent.aggregate_kind
       AND binding.stream_id IS intent.thread_id
       AND binding.stream_version IS intent.binding_event_stream_version
       AND binding.event_type IS intent.binding_event_type
       AND binding.sequence IS intent.binding_event_sequence
       AND binding.occurred_at IS intent.created_at
      INNER JOIN projection_threads projection
        ON projection.thread_id IS intent.thread_id
      WHERE intent.command_id IS NEW.command_id
        AND intent.command_type IS NEW.command_type
        AND intent.authority IS NEW.authority
        AND intent.aggregate_kind IS NEW.aggregate_kind
        AND intent.thread_id IS NEW.thread_id
        AND intent.command_fingerprint IS NEW.command_fingerprint
        AND intent.receipt_result_sequence IS NEW.result_sequence
        AND intent.receipt_accepted_at IS NEW.accepted_at
        AND intent.receipt_status IS NEW.status
        AND intent.receipt_status IS 'accepted'
        AND intent.accepted_receipt_command_id IS intent.command_id
        AND intent.created_event_type IS 'thread.created'
        AND intent.created_event_stream_version IS 1
        AND intent.binding_event_type IS 'thread.agent-control-bound'
        AND intent.binding_event_stream_version IS 2
        AND intent.binding_event_sequence IS intent.created_event_sequence + 1
        AND receipt.result_sequence IS binding.sequence
        AND created.sequence + 1 IS binding.sequence
        AND created.correlation_id IS intent.command_id
        AND created.causation_event_id IS NULL
        AND binding.correlation_id IS intent.command_id
        AND binding.causation_event_id IS NULL
        AND json_type(created.payload_json) IS 'object'
        AND json_type(created.payload_json, '$.threadId') IS 'text'
        AND json_extract(created.payload_json, '$.threadId') IS intent.thread_id
        AND json_type(created.payload_json, '$.projectId') IS 'text'
        AND json_extract(created.payload_json, '$.projectId') IS intent.project_id
        AND json_type(created.payload_json, '$.title') IS 'text'
        AND json_extract(created.payload_json, '$.title') IS intent.title
        AND json_type(created.payload_json, '$.modelSelection') IS 'object'
        AND json(json_extract(created.payload_json, '$.modelSelection'))
          IS json(intent.model_selection_json)
        AND json_type(created.payload_json, '$.runtimeMode') IS 'text'
        AND json_extract(created.payload_json, '$.runtimeMode') IS intent.runtime_mode
        AND json_type(created.payload_json, '$.interactionMode') IS 'text'
        AND json_extract(created.payload_json, '$.interactionMode') IS intent.interaction_mode
        AND json_type(created.payload_json, '$.branch') IS 'text'
        AND json_extract(created.payload_json, '$.branch') IS intent.branch
        AND json_type(created.payload_json, '$.worktreePath') IS 'text'
        AND json_extract(created.payload_json, '$.worktreePath') IS intent.worktree_path
        AND json_type(created.payload_json, '$.createdAt') IS 'text'
        AND json_extract(created.payload_json, '$.createdAt') IS intent.created_at
        AND json_type(created.payload_json, '$.updatedAt') IS 'text'
        AND json_extract(created.payload_json, '$.updatedAt') IS intent.created_at
        AND json_type(binding.payload_json) IS 'object'
        AND json_type(binding.payload_json, '$.threadId') IS 'text'
        AND json_extract(binding.payload_json, '$.threadId') IS intent.thread_id
        AND json_type(binding.payload_json, '$.binding') IS 'object'
        AND json(json_extract(binding.payload_json, '$.binding')) IS json(intent.binding_json)
        AND json_type(binding.payload_json, '$.updatedAt') IS 'text'
        AND json_extract(binding.payload_json, '$.updatedAt') IS intent.created_at
        AND projection.project_id IS intent.project_id
        AND projection.title IS intent.title
        AND json(projection.model_selection_json) IS json(intent.model_selection_json)
        AND projection.runtime_mode IS intent.runtime_mode
        AND projection.interaction_mode IS intent.interaction_mode
        AND projection.branch IS intent.branch
        AND projection.worktree_path IS intent.worktree_path
        AND json(projection.agent_control_json) IS json(intent.binding_json)
        AND json_type(projection.agent_control_json, '$.taskId') IS 'text'
        AND json_extract(projection.agent_control_json, '$.taskId') IS intent.task_id
        AND json_type(projection.agent_control_json, '$.stageRunId') IS 'text'
        AND json_extract(projection.agent_control_json, '$.stageRunId') IS intent.stage_run_id
        AND json_type(projection.agent_control_json, '$.attemptId') IS 'text'
        AND json_extract(projection.agent_control_json, '$.attemptId') IS intent.attempt_id
        AND json_type(projection.agent_control_json, '$.roleId') IS 'text'
        AND json_extract(projection.agent_control_json, '$.roleId') IS intent.role_id
        AND json_type(projection.agent_control_json, '$.controlState') IS 'text'
        AND json_extract(projection.agent_control_json, '$.controlState') IS 'controlled'
        AND projection.latest_turn_id IS NULL
        AND projection.created_at IS intent.created_at
        AND projection.updated_at IS intent.created_at
        AND projection.archived_at IS NULL
        AND projection.latest_user_message_at IS NULL
        AND projection.pending_approval_count IS 0
        AND projection.pending_user_input_count IS 0
        AND projection.has_actionable_proposed_plan IS 0
        AND projection.deleted_at IS NULL
        AND (
          SELECT COUNT(*)
          FROM orchestration_events event
          WHERE event.command_id IS intent.command_id
        ) IS 2
        AND (
          SELECT COUNT(*)
          FROM orchestration_agent_control_thread_materialization_intents candidate
          WHERE candidate.command_id IS intent.command_id
        ) IS 1
        AND (
          SELECT COUNT(*)
          FROM orchestration_command_receipts candidate
          WHERE candidate.command_id IS intent.command_id
        ) IS 1
        AND (
          SELECT COUNT(*)
          FROM projection_threads candidate
          WHERE candidate.thread_id IS intent.thread_id
        ) IS 1
    )
    BEGIN
      SELECT RAISE(
        ABORT,
        'controlled thread materialization accepted evidence is incomplete'
      );
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_materialization_receipt_immutable_update
    BEFORE UPDATE ON orchestration_command_receipts
    WHEN EXISTS (
      SELECT 1
      FROM orchestration_agent_control_thread_materialization_intents intent
      WHERE intent.command_id IS OLD.command_id
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
      FROM orchestration_agent_control_thread_materialization_intents intent
      WHERE intent.command_id IS OLD.command_id
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
      FROM orchestration_agent_control_thread_materialization_receipts evidence
      WHERE evidence.command_id IS OLD.command_id
         OR evidence.command_id IS NEW.command_id
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
      FROM orchestration_agent_control_thread_materialization_receipts evidence
      WHERE evidence.command_id IS OLD.command_id
    )
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread materialization event is immutable');
    END
  `;
  yield* sql`
    CREATE TRIGGER trg_orchestration_materialization_event_insert_after_acceptance
    BEFORE INSERT ON orchestration_events
    WHEN NEW.command_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM orchestration_agent_control_thread_materialization_receipts evidence
        WHERE evidence.command_id IS NEW.command_id
      )
    BEGIN
      SELECT RAISE(
        ABORT,
        'controlled thread materialization command already finalized'
      );
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
