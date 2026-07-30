import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Durable, prepare-specific post-commit finalization evidence.
 *
 * Migration 050 is itself the immutable cutover. Accepted Prepare receipts
 * present while the migration runs are classified as legacy and can never
 * acquire an obligation. Accepted Prepare receipts inserted after the
 * migration must close the reciprocal obligation/evidence/finalization/marker
 * chain in the same transaction.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE agent_control_controlled_thread_prepare_legacy_acceptances (
      prepare_command_id TEXT PRIMARY KEY,
      FOREIGN KEY (prepare_command_id)
      REFERENCES agent_control_command_receipts(command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    INSERT INTO agent_control_controlled_thread_prepare_legacy_acceptances (
      prepare_command_id
    )
    SELECT receipt.command_id
    FROM agent_control_command_receipts receipt
    JOIN agent_control_controlled_thread_command_intents intent
      ON intent.command_id = receipt.command_id
    WHERE intent.command_type =
        'agentControl.controlledThreadReservation.prepare'
      AND intent.aggregate_kind = 'controlled-thread-reservation'
      AND receipt.aggregate_kind = 'controlled-thread-reservation'
      AND receipt.status = 'accepted'
  `;

  yield* sql`
    CREATE TABLE agent_control_controlled_thread_prepare_acceptance_obligations (
      prepare_command_id TEXT PRIMARY KEY,
      prepare_command_fingerprint TEXT NOT NULL CHECK (
        length(prepare_command_fingerprint) = 64
        AND prepare_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      authority TEXT NOT NULL CHECK (authority = 'controller'),
      aggregate_kind TEXT NOT NULL CHECK (
        aggregate_kind = 'controlled-thread-reservation'
      ),
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      receipt_command_id TEXT NOT NULL UNIQUE CHECK (
        receipt_command_id = prepare_command_id
      ),
      receipt_status TEXT NOT NULL CHECK (receipt_status = 'accepted'),
      receipt_result_sequence NOT NULL CHECK (
        typeof(receipt_result_sequence) = 'integer'
        AND receipt_result_sequence >= 1
      ),
      receipt_result_stream_version NOT NULL CHECK (
        typeof(receipt_result_stream_version) = 'integer'
        AND receipt_result_stream_version = 1
      ),
      receipt_event_created NOT NULL CHECK (
        typeof(receipt_event_created) = 'integer'
        AND receipt_event_created = 1
      ),
      receipt_accepted_at TEXT NOT NULL,
      FOREIGN KEY (prepare_command_id)
      REFERENCES agent_control_controlled_thread_command_intents(command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (receipt_command_id)
      REFERENCES agent_control_command_receipts(command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (prepare_command_id)
      REFERENCES agent_control_controlled_thread_prepare_accepted_evidence(
        prepare_command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_controlled_thread_prepare_accepted_evidence (
      prepare_command_id TEXT PRIMARY KEY,
      prepare_command_fingerprint TEXT NOT NULL CHECK (
        length(prepare_command_fingerprint) = 64
        AND prepare_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      authority TEXT NOT NULL CHECK (authority = 'controller'),
      aggregate_kind TEXT NOT NULL CHECK (
        aggregate_kind = 'controlled-thread-reservation'
      ),
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      prepared_event_id TEXT NOT NULL UNIQUE,
      prepared_stream_version NOT NULL CHECK (
        typeof(prepared_stream_version) = 'integer'
        AND prepared_stream_version = 1
      ),
      prepared_event_sequence NOT NULL CHECK (
        typeof(prepared_event_sequence) = 'integer'
        AND prepared_event_sequence >= 1
      ),
      receipt_command_id TEXT NOT NULL UNIQUE CHECK (
        receipt_command_id = prepare_command_id
      ),
      receipt_status TEXT NOT NULL CHECK (receipt_status = 'accepted'),
      receipt_result_sequence NOT NULL CHECK (
        typeof(receipt_result_sequence) = 'integer'
        AND receipt_result_sequence >= 1
        AND receipt_result_sequence = prepared_event_sequence
      ),
      receipt_result_stream_version NOT NULL CHECK (
        typeof(receipt_result_stream_version) = 'integer'
        AND receipt_result_stream_version = prepared_stream_version
      ),
      receipt_event_created NOT NULL CHECK (
        typeof(receipt_event_created) = 'integer'
        AND receipt_event_created = 1
      ),
      receipt_accepted_at TEXT NOT NULL,
      FOREIGN KEY (prepare_command_id)
      REFERENCES agent_control_controlled_thread_prepare_acceptance_obligations(
        prepare_command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (
        controlled_thread_reservation_id, prepared_stream_version
      )
      REFERENCES agent_control_controlled_thread_stream_catalog(
        controlled_thread_reservation_id, stream_version
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (prepared_event_id)
      REFERENCES agent_control_events(event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (prepare_command_id)
      REFERENCES agent_control_controlled_thread_prepare_finalizations(
        prepare_command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_controlled_thread_prepare_finalizations (
      prepare_command_id TEXT PRIMARY KEY,
      prepare_command_fingerprint TEXT NOT NULL CHECK (
        length(prepare_command_fingerprint) = 64
        AND prepare_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      authority TEXT NOT NULL CHECK (authority = 'controller'),
      aggregate_kind TEXT NOT NULL CHECK (
        aggregate_kind = 'controlled-thread-reservation'
      ),
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      prepared_event_id TEXT NOT NULL UNIQUE,
      prepared_stream_version NOT NULL CHECK (
        typeof(prepared_stream_version) = 'integer'
        AND prepared_stream_version = 1
      ),
      prepared_event_sequence NOT NULL CHECK (
        typeof(prepared_event_sequence) = 'integer'
        AND prepared_event_sequence >= 1
      ),
      receipt_command_id TEXT NOT NULL UNIQUE CHECK (
        receipt_command_id = prepare_command_id
      ),
      receipt_status TEXT NOT NULL CHECK (receipt_status = 'accepted'),
      receipt_result_sequence NOT NULL CHECK (
        typeof(receipt_result_sequence) = 'integer'
        AND receipt_result_sequence >= 1
        AND receipt_result_sequence = prepared_event_sequence
      ),
      receipt_result_stream_version NOT NULL CHECK (
        typeof(receipt_result_stream_version) = 'integer'
        AND receipt_result_stream_version = prepared_stream_version
      ),
      receipt_event_created NOT NULL CHECK (
        typeof(receipt_event_created) = 'integer'
        AND receipt_event_created = 1
      ),
      receipt_accepted_at TEXT NOT NULL,
      initial_finalization_owner_id TEXT NOT NULL CHECK (
        length(initial_finalization_owner_id) = 36
        AND initial_finalization_owner_id
          GLOB '????????-????-????-????-????????????'
        AND initial_finalization_owner_id NOT GLOB '*[^0-9a-f-]*'
      ),
      initial_status TEXT NOT NULL CHECK (initial_status = 'pending'),
      initial_revision NOT NULL CHECK (
        typeof(initial_revision) = 'integer'
        AND initial_revision = 0
      ),
      finalization_owner_id TEXT NOT NULL CHECK (
        length(finalization_owner_id) = 36
        AND finalization_owner_id GLOB '????????-????-????-????-????????????'
        AND finalization_owner_id NOT GLOB '*[^0-9a-f-]*'
      ),
      status TEXT NOT NULL CHECK (status IN ('pending', 'claimed', 'completed')),
      revision NOT NULL CHECK (
        typeof(revision) = 'integer'
        AND revision >= 0
      ),
      claimed_at TEXT,
      completed_at TEXT,
      CHECK (
        (status = 'pending' AND revision = 0
          AND claimed_at IS NULL AND completed_at IS NULL)
        OR
        (status = 'claimed' AND revision >= 1
          AND claimed_at IS NOT NULL AND completed_at IS NULL)
        OR
        (status = 'completed' AND revision >= 2
          AND claimed_at IS NOT NULL AND completed_at IS NOT NULL
          AND claimed_at <= completed_at)
      ),
      FOREIGN KEY (prepare_command_id)
      REFERENCES agent_control_controlled_thread_prepare_accepted_evidence(
        prepare_command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (prepare_command_id)
      REFERENCES agent_control_controlled_thread_prepare_final_commit_markers(
        prepare_command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_controlled_thread_prepare_final_commit_markers (
      prepare_command_id TEXT PRIMARY KEY,
      prepare_command_fingerprint TEXT NOT NULL CHECK (
        length(prepare_command_fingerprint) = 64
        AND prepare_command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      authority TEXT NOT NULL CHECK (authority = 'controller'),
      aggregate_kind TEXT NOT NULL CHECK (
        aggregate_kind = 'controlled-thread-reservation'
      ),
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      prepared_event_id TEXT NOT NULL UNIQUE,
      prepared_stream_version NOT NULL CHECK (
        typeof(prepared_stream_version) = 'integer'
        AND prepared_stream_version = 1
      ),
      prepared_event_sequence NOT NULL CHECK (
        typeof(prepared_event_sequence) = 'integer'
        AND prepared_event_sequence >= 1
      ),
      receipt_command_id TEXT NOT NULL UNIQUE CHECK (
        receipt_command_id = prepare_command_id
      ),
      receipt_status TEXT NOT NULL CHECK (receipt_status = 'accepted'),
      receipt_result_sequence NOT NULL CHECK (
        typeof(receipt_result_sequence) = 'integer'
        AND receipt_result_sequence >= 1
        AND receipt_result_sequence = prepared_event_sequence
      ),
      receipt_result_stream_version NOT NULL CHECK (
        typeof(receipt_result_stream_version) = 'integer'
        AND receipt_result_stream_version = prepared_stream_version
      ),
      receipt_event_created NOT NULL CHECK (
        typeof(receipt_event_created) = 'integer'
        AND receipt_event_created = 1
      ),
      receipt_accepted_at TEXT NOT NULL,
      finalization_owner_id TEXT NOT NULL CHECK (
        length(finalization_owner_id) = 36
        AND finalization_owner_id GLOB '????????-????-????-????-????????????'
        AND finalization_owner_id NOT GLOB '*[^0-9a-f-]*'
      ),
      finalization_status TEXT NOT NULL CHECK (finalization_status = 'pending'),
      finalization_revision NOT NULL CHECK (
        typeof(finalization_revision) = 'integer'
        AND finalization_revision = 0
      ),
      FOREIGN KEY (prepare_command_id)
      REFERENCES agent_control_controlled_thread_prepare_finalizations(
        prepare_command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

  const insertPrepareAcceptanceObligation = `
    INSERT OR IGNORE INTO
      agent_control_controlled_thread_prepare_acceptance_obligations (
        prepare_command_id, prepare_command_fingerprint, authority,
        aggregate_kind, project_id, task_id,
        controlled_thread_reservation_id, receipt_command_id, receipt_status,
        receipt_result_sequence, receipt_result_stream_version,
        receipt_event_created, receipt_accepted_at
      )
    SELECT
      intent.command_id, intent.request_fingerprint, intent.authority,
      intent.aggregate_kind, intent.project_id, intent.task_id,
      intent.aggregate_id, receipt.command_id, receipt.status,
      receipt.result_sequence, receipt.result_stream_version,
      receipt.event_created, receipt.accepted_at
    FROM agent_control_controlled_thread_command_intents intent
    JOIN agent_control_command_receipts receipt
      ON receipt.command_id = intent.command_id
    WHERE receipt.command_id = NEW.command_id
      AND intent.command_type =
        'agentControl.controlledThreadReservation.prepare'
      AND intent.aggregate_kind = 'controlled-thread-reservation'
      AND receipt.status = 'accepted'
  `;
  const insertPrepareAcceptedEvidence = `
    INSERT OR IGNORE INTO
      agent_control_controlled_thread_prepare_accepted_evidence (
        prepare_command_id, prepare_command_fingerprint, authority,
        aggregate_kind, project_id, task_id,
        controlled_thread_reservation_id, prepared_event_id,
        prepared_stream_version, prepared_event_sequence,
        receipt_command_id, receipt_status, receipt_result_sequence,
        receipt_result_stream_version, receipt_event_created,
        receipt_accepted_at
      )
    SELECT
      intent.command_id, intent.request_fingerprint, intent.authority,
      intent.aggregate_kind, intent.project_id, intent.task_id,
      intent.aggregate_id, catalog.event_id, catalog.stream_version,
      event.sequence, receipt.command_id, receipt.status,
      receipt.result_sequence, receipt.result_stream_version,
      receipt.event_created, receipt.accepted_at
    FROM agent_control_controlled_thread_command_intents intent
    JOIN agent_control_command_receipts receipt
      ON receipt.command_id = intent.command_id
    JOIN agent_control_controlled_thread_stream_catalog catalog
      ON catalog.command_id = intent.command_id
     AND catalog.stream_version = 1
    JOIN agent_control_events event
      ON event.event_id = catalog.event_id
    WHERE receipt.command_id = NEW.command_id
      AND intent.command_type =
        'agentControl.controlledThreadReservation.prepare'
      AND intent.aggregate_kind = 'controlled-thread-reservation'
      AND receipt.status = 'accepted'
  `;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_controlled_thread_prepare_receipt_acceptance_capture
    AFTER INSERT ON agent_control_command_receipts
    BEGIN
      ${insertPrepareAcceptanceObligation};
      ${insertPrepareAcceptedEvidence};
    END
  `).unprepared;

  yield* sql`
    CREATE INDEX idx_agent_control_controlled_thread_prepare_finalizations_open
    ON agent_control_controlled_thread_prepare_finalizations(status, revision)
    WHERE status <> 'completed'
  `;

  for (const table of [
    "agent_control_controlled_thread_prepare_acceptance_obligations",
    "agent_control_controlled_thread_prepare_accepted_evidence",
    "agent_control_controlled_thread_prepare_finalizations",
    "agent_control_controlled_thread_prepare_final_commit_markers",
  ]) {
    yield* sql.unsafe(`
      CREATE TRIGGER ${table}_reject_legacy
      BEFORE INSERT ON ${table}
      WHEN EXISTS (
        SELECT 1
        FROM agent_control_controlled_thread_prepare_legacy_acceptances legacy
        WHERE legacy.prepare_command_id = NEW.prepare_command_id
      )
      BEGIN
        SELECT RAISE(
          ABORT,
          'legacy controlled thread Prepare acceptance cannot be finalized'
        );
      END
    `).unprepared;
  }

  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_finalization_insert_validate
    BEFORE INSERT ON agent_control_controlled_thread_prepare_finalizations
    WHEN NEW.initial_finalization_owner_id IS NOT NEW.finalization_owner_id
      OR NEW.initial_status IS NOT 'pending'
      OR NEW.initial_revision IS NOT 0
      OR NEW.status IS NOT 'pending'
      OR NEW.revision IS NOT 0
      OR NEW.claimed_at IS NOT NULL
      OR NEW.completed_at IS NOT NULL
    BEGIN
      SELECT RAISE(
        ABORT,
        'controlled thread Prepare finalization must start pending at revision zero'
      );
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_commit_marker_validate
    BEFORE INSERT ON agent_control_controlled_thread_prepare_final_commit_markers
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_controlled_thread_prepare_acceptance_obligations obligation
      JOIN agent_control_controlled_thread_prepare_accepted_evidence evidence
        ON evidence.prepare_command_id IS obligation.prepare_command_id
       AND evidence.prepare_command_fingerprint IS
         obligation.prepare_command_fingerprint
       AND evidence.authority IS obligation.authority
       AND evidence.aggregate_kind IS obligation.aggregate_kind
       AND evidence.project_id IS obligation.project_id
       AND evidence.task_id IS obligation.task_id
       AND evidence.controlled_thread_reservation_id IS
         obligation.controlled_thread_reservation_id
       AND evidence.receipt_command_id IS obligation.receipt_command_id
       AND evidence.receipt_status IS obligation.receipt_status
       AND evidence.receipt_result_sequence IS
         obligation.receipt_result_sequence
       AND evidence.receipt_result_stream_version IS
         obligation.receipt_result_stream_version
       AND evidence.receipt_event_created IS obligation.receipt_event_created
       AND evidence.receipt_accepted_at IS obligation.receipt_accepted_at
      JOIN agent_control_controlled_thread_prepare_finalizations finalization
        ON finalization.prepare_command_id IS evidence.prepare_command_id
       AND finalization.prepare_command_fingerprint IS
         evidence.prepare_command_fingerprint
       AND finalization.authority IS evidence.authority
       AND finalization.aggregate_kind IS evidence.aggregate_kind
       AND finalization.project_id IS evidence.project_id
       AND finalization.task_id IS evidence.task_id
       AND finalization.controlled_thread_reservation_id IS
         evidence.controlled_thread_reservation_id
       AND finalization.prepared_event_id IS evidence.prepared_event_id
       AND finalization.prepared_stream_version IS
         evidence.prepared_stream_version
       AND finalization.prepared_event_sequence IS
         evidence.prepared_event_sequence
       AND finalization.receipt_command_id IS evidence.receipt_command_id
       AND finalization.receipt_status IS evidence.receipt_status
       AND finalization.receipt_result_sequence IS
         evidence.receipt_result_sequence
       AND finalization.receipt_result_stream_version IS
         evidence.receipt_result_stream_version
       AND finalization.receipt_event_created IS evidence.receipt_event_created
       AND finalization.receipt_accepted_at IS evidence.receipt_accepted_at
      JOIN agent_control_controlled_thread_command_intents intent
        ON intent.command_id IS obligation.prepare_command_id
       AND intent.request_fingerprint IS obligation.prepare_command_fingerprint
       AND intent.command_type IS
         'agentControl.controlledThreadReservation.prepare'
       AND intent.authority IS obligation.authority
       AND intent.aggregate_kind IS obligation.aggregate_kind
       AND intent.aggregate_id IS obligation.controlled_thread_reservation_id
       AND intent.project_id IS obligation.project_id
       AND intent.task_id IS obligation.task_id
      JOIN agent_control_command_receipts receipt
        ON receipt.command_id IS obligation.receipt_command_id
       AND receipt.command_fingerprint IS
         obligation.prepare_command_fingerprint
       AND receipt.authority IS obligation.authority
       AND receipt.aggregate_kind IS obligation.aggregate_kind
       AND receipt.aggregate_id IS obligation.controlled_thread_reservation_id
       AND receipt.status IS obligation.receipt_status
       AND receipt.result_sequence IS obligation.receipt_result_sequence
       AND receipt.result_stream_version IS
         obligation.receipt_result_stream_version
       AND receipt.event_created IS obligation.receipt_event_created
       AND receipt.accepted_at IS obligation.receipt_accepted_at
       AND receipt.error_code IS NULL
      JOIN agent_control_controlled_thread_stream_catalog catalog
        ON catalog.command_id IS obligation.prepare_command_id
       AND catalog.controlled_thread_reservation_id IS
         obligation.controlled_thread_reservation_id
       AND catalog.event_id IS evidence.prepared_event_id
       AND catalog.stream_version IS evidence.prepared_stream_version
       AND catalog.event_type IS
         'agentControl.controlledThreadReservation.prepared'
       AND catalog.project_id IS obligation.project_id
       AND catalog.task_id IS obligation.task_id
      JOIN agent_control_events event
        ON event.event_id IS evidence.prepared_event_id
       AND event.aggregate_kind IS obligation.aggregate_kind
       AND event.stream_id IS obligation.controlled_thread_reservation_id
       AND event.stream_version IS evidence.prepared_stream_version
       AND event.event_type IS catalog.event_type
       AND event.command_id IS obligation.prepare_command_id
       AND event.sequence IS evidence.prepared_event_sequence
       AND event.correlation_id IS obligation.prepare_command_id
       AND event.causation_event_id IS NULL
       AND event.actor_authority IS obligation.authority
       AND event.occurred_at IS obligation.receipt_accepted_at
      WHERE obligation.prepare_command_id IS NEW.prepare_command_id
        AND obligation.prepare_command_fingerprint IS
          NEW.prepare_command_fingerprint
        AND obligation.authority IS NEW.authority
        AND obligation.aggregate_kind IS NEW.aggregate_kind
        AND obligation.project_id IS NEW.project_id
        AND obligation.task_id IS NEW.task_id
        AND obligation.controlled_thread_reservation_id IS
          NEW.controlled_thread_reservation_id
        AND evidence.prepared_event_id IS NEW.prepared_event_id
        AND evidence.prepared_stream_version IS NEW.prepared_stream_version
        AND evidence.prepared_event_sequence IS NEW.prepared_event_sequence
        AND obligation.receipt_command_id IS NEW.receipt_command_id
        AND obligation.receipt_status IS NEW.receipt_status
        AND obligation.receipt_result_sequence IS NEW.receipt_result_sequence
        AND obligation.receipt_result_stream_version IS
          NEW.receipt_result_stream_version
        AND obligation.receipt_event_created IS NEW.receipt_event_created
        AND obligation.receipt_accepted_at IS NEW.receipt_accepted_at
        AND finalization.initial_finalization_owner_id IS
          NEW.finalization_owner_id
        AND finalization.initial_status IS NEW.finalization_status
        AND finalization.initial_revision IS NEW.finalization_revision
        AND finalization.status IS 'pending'
        AND finalization.revision IS 0
        AND finalization.claimed_at IS NULL
        AND finalization.completed_at IS NULL
        AND NOT EXISTS (
          SELECT 1
          FROM agent_control_controlled_thread_prepare_legacy_acceptances legacy
          WHERE legacy.prepare_command_id IS NEW.prepare_command_id
        )
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
        'controlled thread Prepare commit evidence is incomplete'
      );
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_finalization_identity_no_update
    BEFORE UPDATE ON agent_control_controlled_thread_prepare_finalizations
    WHEN OLD.prepare_command_id IS NOT NEW.prepare_command_id
      OR OLD.prepare_command_fingerprint IS NOT NEW.prepare_command_fingerprint
      OR OLD.authority IS NOT NEW.authority
      OR OLD.aggregate_kind IS NOT NEW.aggregate_kind
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
      OR OLD.initial_finalization_owner_id IS NOT
        NEW.initial_finalization_owner_id
      OR OLD.initial_status IS NOT NEW.initial_status
      OR OLD.initial_revision IS NOT NEW.initial_revision
    BEGIN
      SELECT RAISE(
        ABORT,
        'controlled thread Prepare finalization identity is immutable'
      );
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
      SELECT RAISE(
        ABORT,
        'invalid controlled thread Prepare finalization transition'
      );
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_finalization_completed_no_update
    BEFORE UPDATE ON agent_control_controlled_thread_prepare_finalizations
    WHEN OLD.status IS 'completed'
    BEGIN
      SELECT RAISE(
        ABORT,
        'completed controlled thread Prepare finalization is immutable'
      );
    END
  `;

  for (const table of [
    "agent_control_controlled_thread_prepare_legacy_acceptances",
    "agent_control_controlled_thread_prepare_acceptance_obligations",
    "agent_control_controlled_thread_prepare_accepted_evidence",
    "agent_control_controlled_thread_prepare_final_commit_markers",
  ]) {
    yield* sql.unsafe(`
      CREATE TRIGGER ${table}_no_update
      BEFORE UPDATE ON ${table}
      BEGIN
        SELECT RAISE(ABORT, 'controlled thread Prepare evidence is immutable');
      END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER ${table}_no_delete
      BEFORE DELETE ON ${table}
      BEGIN
        SELECT RAISE(ABORT, 'controlled thread Prepare evidence is immutable');
      END
    `).unprepared;
  }
  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_legacy_acceptance_no_insert
    BEFORE INSERT ON agent_control_controlled_thread_prepare_legacy_acceptances
    BEGIN
      SELECT RAISE(
        ABORT,
        'controlled thread Prepare legacy cutover is immutable'
      );
    END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_controlled_thread_prepare_finalization_no_delete
    BEFORE DELETE ON agent_control_controlled_thread_prepare_finalizations
    BEGIN
      SELECT RAISE(
        ABORT,
        'controlled thread Prepare finalization evidence is immutable'
      );
    END
  `;
});
