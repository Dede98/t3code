import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Durable, prepare-specific post-commit finalization evidence.
 *
 * Existing accepted prepare receipts are intentionally not backfilled: without
 * proof that their historical hot event was (or was not) published, turning
 * them into a recoverable outbox would permit a blind duplicate publication.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE agent_control_controlled_thread_prepare_finalizations (
      prepare_command_id TEXT PRIMARY KEY,
      prepare_command_fingerprint TEXT NOT NULL CHECK (
        length(prepare_command_fingerprint) = 64
        AND prepare_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      prepared_event_id TEXT NOT NULL UNIQUE,
      prepared_stream_version INTEGER NOT NULL CHECK (prepared_stream_version = 1),
      prepared_event_sequence INTEGER NOT NULL CHECK (prepared_event_sequence >= 1),
      receipt_command_id TEXT NOT NULL UNIQUE,
      receipt_status TEXT NOT NULL CHECK (receipt_status = 'accepted'),
      receipt_result_sequence INTEGER NOT NULL CHECK (
        receipt_result_sequence = prepared_event_sequence
      ),
      receipt_result_stream_version INTEGER NOT NULL CHECK (
        receipt_result_stream_version = prepared_stream_version
      ),
      receipt_event_created INTEGER NOT NULL CHECK (receipt_event_created = 1),
      receipt_accepted_at TEXT NOT NULL,
      finalization_owner_id TEXT NOT NULL CHECK (
        length(finalization_owner_id) = 36
        AND finalization_owner_id GLOB '????????-????-????-????-????????????'
        AND finalization_owner_id NOT GLOB '*[^0-9a-f-]*'
      ),
      status TEXT NOT NULL CHECK (status IN ('pending', 'claimed', 'completed')),
      revision INTEGER NOT NULL CHECK (revision >= 0),
      claimed_at TEXT,
      completed_at TEXT,
      CHECK (
        prepare_command_id = receipt_command_id
        AND (
          (status = 'pending' AND revision = 0
            AND claimed_at IS NULL AND completed_at IS NULL)
          OR
          (status = 'claimed' AND revision >= 1
            AND claimed_at IS NOT NULL AND completed_at IS NULL)
          OR
          (status = 'completed' AND revision >= 2
            AND claimed_at IS NOT NULL AND completed_at IS NOT NULL
            AND claimed_at <= completed_at)
        )
      ),
      FOREIGN KEY (prepare_command_id)
      REFERENCES agent_control_controlled_thread_command_intents(command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (receipt_command_id)
      REFERENCES agent_control_command_receipts(command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (
        controlled_thread_reservation_id, prepared_stream_version
      )
      REFERENCES agent_control_controlled_thread_stream_catalog(
        controlled_thread_reservation_id, stream_version
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (prepared_event_id)
      REFERENCES agent_control_events(event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

  yield* sql`
    CREATE INDEX idx_agent_control_controlled_thread_prepare_finalizations_open
    ON agent_control_controlled_thread_prepare_finalizations(status, revision)
    WHERE status <> 'completed'
  `;

  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_finalization_insert_validate
    BEFORE INSERT ON agent_control_controlled_thread_prepare_finalizations
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_controlled_thread_command_intents intent
      JOIN agent_control_command_receipts receipt
        ON receipt.command_id IS intent.command_id
       AND receipt.command_fingerprint IS intent.request_fingerprint
       AND receipt.aggregate_kind IS intent.aggregate_kind
       AND receipt.aggregate_id IS intent.aggregate_id
      JOIN agent_control_controlled_thread_stream_catalog catalog
        ON catalog.command_id IS intent.command_id
       AND catalog.controlled_thread_reservation_id IS intent.aggregate_id
       AND catalog.stream_version IS 1
       AND catalog.event_type IS
         'agentControl.controlledThreadReservation.prepared'
      JOIN agent_control_events event
        ON event.event_id IS catalog.event_id
       AND event.aggregate_kind IS 'controlled-thread-reservation'
       AND event.stream_id IS catalog.controlled_thread_reservation_id
       AND event.stream_version IS catalog.stream_version
       AND event.event_type IS catalog.event_type
       AND event.command_id IS catalog.command_id
      WHERE intent.command_id IS NEW.prepare_command_id
        AND intent.request_fingerprint IS NEW.prepare_command_fingerprint
        AND intent.command_type IS
          'agentControl.controlledThreadReservation.prepare'
        AND intent.authority IS 'controller'
        AND intent.aggregate_kind IS 'controlled-thread-reservation'
        AND intent.aggregate_id IS NEW.controlled_thread_reservation_id
        AND intent.project_id IS NEW.project_id
        AND intent.task_id IS NEW.task_id
        AND receipt.authority IS 'controller'
        AND receipt.status IS NEW.receipt_status
        AND receipt.result_sequence IS NEW.receipt_result_sequence
        AND receipt.result_stream_version IS NEW.receipt_result_stream_version
        AND receipt.event_created IS NEW.receipt_event_created
        AND receipt.accepted_at IS NEW.receipt_accepted_at
        AND receipt.error_code IS NULL
        AND catalog.event_id IS NEW.prepared_event_id
        AND catalog.stream_version IS NEW.prepared_stream_version
        AND catalog.project_id IS NEW.project_id
        AND catalog.task_id IS NEW.task_id
        AND event.sequence IS NEW.prepared_event_sequence
        AND event.correlation_id IS NEW.prepare_command_id
        AND event.causation_event_id IS NULL
        AND event.actor_authority IS 'controller'
        AND event.occurred_at IS NEW.receipt_accepted_at
        AND (
          SELECT count(*)
          FROM agent_control_events candidate
          WHERE candidate.aggregate_kind IS 'controlled-thread-reservation'
            AND candidate.stream_id IS NEW.controlled_thread_reservation_id
            AND candidate.stream_version IS 1
        ) IS 1
    )
    BEGIN
      SELECT RAISE(
        ABORT,
        'controlled thread prepare finalization evidence is incomplete'
      );
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_finalization_identity_no_update
    BEFORE UPDATE ON agent_control_controlled_thread_prepare_finalizations
    WHEN OLD.prepare_command_id IS NOT NEW.prepare_command_id
      OR OLD.prepare_command_fingerprint IS NOT NEW.prepare_command_fingerprint
      OR OLD.project_id IS NOT NEW.project_id
      OR OLD.task_id IS NOT NEW.task_id
      OR OLD.controlled_thread_reservation_id IS NOT
        NEW.controlled_thread_reservation_id
      OR OLD.prepared_event_id IS NOT NEW.prepared_event_id
      OR OLD.prepared_stream_version IS NOT NEW.prepared_stream_version
      OR OLD.prepared_event_sequence IS NOT NEW.prepared_event_sequence
      OR OLD.receipt_command_id IS NOT NEW.receipt_command_id
      OR OLD.receipt_status IS NOT NEW.receipt_status
      OR OLD.receipt_result_sequence IS NOT NEW.receipt_result_sequence
      OR OLD.receipt_result_stream_version IS NOT
        NEW.receipt_result_stream_version
      OR OLD.receipt_event_created IS NOT NEW.receipt_event_created
      OR OLD.receipt_accepted_at IS NOT NEW.receipt_accepted_at
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread prepare finalization identity is immutable');
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_finalization_transition_validate
    BEFORE UPDATE ON agent_control_controlled_thread_prepare_finalizations
    WHEN NOT (
      (OLD.status IS 'pending'
        AND NEW.status IS 'claimed'
        AND NEW.revision IS OLD.revision + 1
        AND NEW.claimed_at IS NOT NULL
        AND NEW.completed_at IS NULL)
      OR
      (OLD.status IS 'claimed'
        AND NEW.status IS 'claimed'
        AND NEW.revision IS OLD.revision + 1
        AND NEW.finalization_owner_id IS NOT OLD.finalization_owner_id
        AND NEW.claimed_at IS NOT NULL
        AND NEW.completed_at IS NULL)
      OR
      (OLD.status IS 'claimed'
        AND NEW.status IS 'completed'
        AND NEW.revision IS OLD.revision + 1
        AND NEW.finalization_owner_id IS OLD.finalization_owner_id
        AND NEW.claimed_at IS OLD.claimed_at
        AND NEW.completed_at IS NOT NULL)
    )
    BEGIN
      SELECT RAISE(ABORT, 'invalid controlled thread prepare finalization transition');
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_finalization_completed_no_update
    BEFORE UPDATE ON agent_control_controlled_thread_prepare_finalizations
    WHEN OLD.status IS 'completed'
    BEGIN
      SELECT RAISE(ABORT, 'completed controlled thread prepare finalization is immutable');
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_finalization_no_delete
    BEFORE DELETE ON agent_control_controlled_thread_prepare_finalizations
    BEGIN
      SELECT RAISE(ABORT, 'controlled thread prepare finalization evidence is immutable');
    END
  `;
});
