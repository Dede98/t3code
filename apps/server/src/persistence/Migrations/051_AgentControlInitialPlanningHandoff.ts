import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const text = (column: string) => `typeof(${column}) = 'text' AND length(${column}) > 0`;
const positiveInteger = (column: string) => `typeof(${column}) = 'integer' AND ${column} >= 1`;
const sha256 = (column: string) =>
  `${text(column)} AND length(${column}) = 64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;
const timestamp = (column: string) => `
  ${text(column)}
  AND length(${column}) = 24
  AND ${column} GLOB
    '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
`;

const modelSelectionPredicate = `
  json_valid(NEW.model_selection_json) = 1
  AND json_type(NEW.model_selection_json) = 'object'
  AND (SELECT count(*) FROM json_each(NEW.model_selection_json)) IN (2, 3)
  AND (SELECT count(*) FROM json_each(NEW.model_selection_json)) =
    (SELECT count(DISTINCT key) FROM json_each(NEW.model_selection_json))
  AND NOT EXISTS (
    SELECT 1 FROM json_each(NEW.model_selection_json)
    WHERE key NOT IN ('instanceId', 'model', 'options')
  )
  AND (
    SELECT count(*) FROM json_each(NEW.model_selection_json)
    WHERE key = 'instanceId'
  ) = 1
  AND json_type(NEW.model_selection_json, '$.instanceId') = 'text'
  AND length(trim(json_extract(NEW.model_selection_json, '$.instanceId'))) > 0
  AND json_extract(NEW.model_selection_json, '$.instanceId') IS NEW.provider_instance_id
  AND (
    SELECT count(*) FROM json_each(NEW.model_selection_json)
    WHERE key = 'model'
  ) = 1
  AND json_type(NEW.model_selection_json, '$.model') = 'text'
  AND length(trim(json_extract(NEW.model_selection_json, '$.model'))) > 0
  AND (
    (
      SELECT count(*) FROM json_each(NEW.model_selection_json)
      WHERE key = 'options'
    ) = 0
    OR (
      (
        SELECT count(*) FROM json_each(NEW.model_selection_json)
        WHERE key = 'options'
      ) = 1
      AND json_type(NEW.model_selection_json, '$.options') = 'array'
      AND NOT EXISTS (
        SELECT 1
        FROM json_each(NEW.model_selection_json, '$.options') option
        WHERE json_type(option.value) <> 'object'
          OR (SELECT count(*) FROM json_each(option.value)) <> 2
          OR (SELECT count(DISTINCT key) FROM json_each(option.value)) <> 2
          OR EXISTS (
            SELECT 1 FROM json_each(option.value)
            WHERE key NOT IN ('id', 'value')
          )
          OR (
            SELECT count(*) FROM json_each(option.value)
            WHERE key = 'id'
          ) <> 1
          OR json_type(option.value, '$.id') <> 'text'
          OR length(trim(json_extract(option.value, '$.id'))) = 0
          OR (
            SELECT count(*) FROM json_each(option.value)
            WHERE key = 'value'
          ) <> 1
          OR json_type(option.value, '$.value') NOT IN ('text', 'true', 'false')
          OR (
            json_type(option.value, '$.value') = 'text'
            AND length(trim(json_extract(option.value, '$.value'))) = 0
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM json_each(NEW.model_selection_json, '$.options') option
        GROUP BY json_extract(option.value, '$.id')
        HAVING count(*) <> 1
      )
    )
  )
`;

/**
 * Immutable initial-planning handoff and mutable provider-delivery recovery.
 *
 * Rows already materialized when this migration runs are permanently classified
 * as legacy. Every later materialization must close the reciprocal handoff
 * intent/receipt/accepted chain before the existing materialization marker can
 * be inserted.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const alreadyApplied = yield* sql<{ readonly count: number }>`
    SELECT count(*) AS count
    FROM sqlite_schema
    WHERE type = 'table'
      AND name = 'agent_control_initial_planning_handoff_accepted'
  `;
  if (alreadyApplied[0]?.count === 1) return;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_legacy_materializations (
      coordinator_command_id PRIMARY KEY CHECK (${sql.literal(text("coordinator_command_id"))}),
      controlled_thread_reservation_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("controlled_thread_reservation_id"))}
      ),
      thread_id UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      materialization_accepted_at NOT NULL CHECK (
        ${sql.literal(timestamp("materialization_accepted_at"))}
      ),
      FOREIGN KEY (coordinator_command_id)
      REFERENCES agent_control_controlled_thread_materialization_accepted(
        coordinator_command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    INSERT INTO agent_control_initial_planning_legacy_materializations (
      coordinator_command_id, controlled_thread_reservation_id, thread_id,
      materialization_accepted_at
    )
    SELECT
      coordinator_command_id, controlled_thread_reservation_id, thread_id,
      accepted_at
    FROM agent_control_controlled_thread_materialization_accepted
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_handoff_intents (
      handoff_id PRIMARY KEY CHECK (${sql.literal(text("handoff_id"))}),
      handoff_fingerprint UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("handoff_fingerprint"))}
      ),
      coordinator_command_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("coordinator_command_id"))}
      ),
      coordinator_command_fingerprint UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("coordinator_command_fingerprint"))}
      ),
      materialization_command_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("materialization_command_id"))}
      ),
      materialization_command_fingerprint UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("materialization_command_fingerprint"))}
      ),
      project_id NOT NULL CHECK (${sql.literal(text("project_id"))}),
      controlled_thread_reservation_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("controlled_thread_reservation_id"))}
      ),
      thread_id UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      task_id NOT NULL CHECK (${sql.literal(text("task_id"))}),
      task_revision NOT NULL CHECK (${sql.literal(positiveInteger("task_revision"))}),
      github_intake_sequence NOT NULL CHECK (
        ${sql.literal(positiveInteger("github_intake_sequence"))}
      ),
      source_identity_fingerprint NOT NULL CHECK (
        ${sql.literal(sha256("source_identity_fingerprint"))}
      ),
      stage_run_id NOT NULL CHECK (${sql.literal(text("stage_run_id"))}),
      attempt_id NOT NULL CHECK (${sql.literal(text("attempt_id"))}),
      role_id NOT NULL CHECK (
        ${sql.literal(text("role_id"))} AND role_id = 'planning'
      ),
      stage_kind NOT NULL CHECK (
        ${sql.literal(text("stage_kind"))} AND stage_kind = 'planning'
      ),
      stage_ordinal NOT NULL CHECK (
        ${sql.literal(positiveInteger("stage_ordinal"))} AND stage_ordinal = 1
      ),
      attempt_ordinal NOT NULL CHECK (
        ${sql.literal(positiveInteger("attempt_ordinal"))} AND attempt_ordinal = 1
      ),
      lease_id NOT NULL CHECK (${sql.literal(text("lease_id"))}),
      lease_holder_id NOT NULL CHECK (${sql.literal(text("lease_holder_id"))}),
      fence_token NOT NULL CHECK (${sql.literal(positiveInteger("fence_token"))}),
      worktree_reservation_id NOT NULL CHECK (
        ${sql.literal(text("worktree_reservation_id"))}
      ),
      worktree_path NOT NULL CHECK (${sql.literal(text("worktree_path"))}),
      provider_instance_id NOT NULL CHECK (${sql.literal(text("provider_instance_id"))}),
      runtime_mode NOT NULL CHECK (
        ${sql.literal(text("runtime_mode"))}
        AND runtime_mode IN ('approval-required', 'full-access')
      ),
      model_selection_json NOT NULL CHECK (
        ${sql.literal(text("model_selection_json"))}
      ),
      planning_role NOT NULL CHECK (
        ${sql.literal(text("planning_role"))} AND planning_role = 'planner'
      ),
      template_version NOT NULL CHECK (
        ${sql.literal(text("template_version"))}
        AND template_version = 'agent-control-initial-planning-prompt-v1'
      ),
      prompt_text NOT NULL CHECK (
        typeof(prompt_text) = 'text'
        AND length(CAST(prompt_text AS BLOB)) BETWEEN 1 AND 65536
      ),
      turn_request_command_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("turn_request_command_id"))}
      ),
      message_id UNIQUE NOT NULL CHECK (${sql.literal(text("message_id"))}),
      provider_delivery_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("provider_delivery_id"))}
      ),
      created_at NOT NULL CHECK (${sql.literal(timestamp("created_at"))}),
      planning_deadline_at NOT NULL CHECK (
        ${sql.literal(timestamp("planning_deadline_at"))}
        AND planning_deadline_at > created_at
      ),
      accepted_marker_handoff_id UNIQUE NOT NULL CHECK (
        accepted_marker_handoff_id = handoff_id
      ),
      UNIQUE (
        handoff_id, handoff_fingerprint, coordinator_command_id,
        coordinator_command_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id,
        created_at
      ),
      FOREIGN KEY (accepted_marker_handoff_id)
      REFERENCES agent_control_initial_planning_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_handoff_receipts (
      handoff_id PRIMARY KEY CHECK (${sql.literal(text("handoff_id"))}),
      handoff_fingerprint UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("handoff_fingerprint"))}
      ),
      coordinator_command_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("coordinator_command_id"))}
      ),
      coordinator_command_fingerprint UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("coordinator_command_fingerprint"))}
      ),
      controlled_thread_reservation_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("controlled_thread_reservation_id"))}
      ),
      thread_id UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      turn_request_command_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("turn_request_command_id"))}
      ),
      message_id UNIQUE NOT NULL CHECK (${sql.literal(text("message_id"))}),
      provider_delivery_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("provider_delivery_id"))}
      ),
      status NOT NULL CHECK (${sql.literal(text("status"))} AND status = 'accepted'),
      accepted_at NOT NULL CHECK (${sql.literal(timestamp("accepted_at"))}),
      accepted_marker_handoff_id UNIQUE NOT NULL CHECK (
        accepted_marker_handoff_id = handoff_id
      ),
      UNIQUE (
        handoff_id, handoff_fingerprint, coordinator_command_id,
        coordinator_command_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id,
        accepted_at
      ),
      UNIQUE (
        handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id
      ),
      FOREIGN KEY (
        handoff_id, handoff_fingerprint, coordinator_command_id,
        coordinator_command_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id,
        accepted_at
      ) REFERENCES agent_control_initial_planning_handoff_intents(
        handoff_id, handoff_fingerprint, coordinator_command_id,
        coordinator_command_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id,
        created_at
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (accepted_marker_handoff_id)
      REFERENCES agent_control_initial_planning_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_handoff_accepted (
      handoff_id PRIMARY KEY CHECK (${sql.literal(text("handoff_id"))}),
      handoff_fingerprint UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("handoff_fingerprint"))}
      ),
      coordinator_command_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("coordinator_command_id"))}
      ),
      coordinator_command_fingerprint UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("coordinator_command_fingerprint"))}
      ),
      controlled_thread_reservation_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("controlled_thread_reservation_id"))}
      ),
      thread_id UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      turn_request_command_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("turn_request_command_id"))}
      ),
      message_id UNIQUE NOT NULL CHECK (${sql.literal(text("message_id"))}),
      provider_delivery_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("provider_delivery_id"))}
      ),
      accepted_at NOT NULL CHECK (${sql.literal(timestamp("accepted_at"))}),
      UNIQUE (
        handoff_id, handoff_fingerprint, coordinator_command_id,
        coordinator_command_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id,
        accepted_at
      ),
      FOREIGN KEY (
        handoff_id, handoff_fingerprint, coordinator_command_id,
        coordinator_command_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id,
        accepted_at
      ) REFERENCES agent_control_initial_planning_handoff_intents(
        handoff_id, handoff_fingerprint, coordinator_command_id,
        coordinator_command_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id,
        created_at
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (
        handoff_id, handoff_fingerprint, coordinator_command_id,
        coordinator_command_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id,
        accepted_at
      ) REFERENCES agent_control_initial_planning_handoff_receipts(
        handoff_id, handoff_fingerprint, coordinator_command_id,
        coordinator_command_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id,
        accepted_at
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (coordinator_command_id)
      REFERENCES agent_control_controlled_thread_materialization_accepted(
        coordinator_command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_initial_planning_accepted_delivery_parent
    ON agent_control_initial_planning_handoff_accepted(
      handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
      thread_id, turn_request_command_id, message_id, provider_delivery_id
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_turn_accepted (
      handoff_id PRIMARY KEY CHECK (${sql.literal(text("handoff_id"))}),
      handoff_fingerprint UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("handoff_fingerprint"))}
      ),
      controlled_thread_reservation_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("controlled_thread_reservation_id"))}
      ),
      thread_id UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      turn_request_command_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("turn_request_command_id"))}
      ),
      message_id UNIQUE NOT NULL CHECK (${sql.literal(text("message_id"))}),
      message_event_id UNIQUE NOT NULL CHECK (${sql.literal(text("message_event_id"))}),
      message_event_sequence UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("message_event_sequence"))}
      ),
      turn_request_event_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("turn_request_event_id"))}
      ),
      turn_request_event_sequence UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("turn_request_event_sequence"))}
        AND turn_request_event_sequence > message_event_sequence
      ),
      receipt_authority NOT NULL CHECK (
        ${sql.literal(text("receipt_authority"))}
        AND receipt_authority = 'agent-control'
      ),
      accepted_at NOT NULL CHECK (${sql.literal(timestamp("accepted_at"))}),
      FOREIGN KEY (handoff_id)
      REFERENCES agent_control_initial_planning_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (turn_request_command_id)
      REFERENCES orchestration_command_receipts(command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (message_event_id)
      REFERENCES orchestration_events(event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (turn_request_event_id)
      REFERENCES orchestration_events(event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_deliveries (
      provider_delivery_id PRIMARY KEY CHECK (
        ${sql.literal(text("provider_delivery_id"))}
      ),
      handoff_id UNIQUE NOT NULL CHECK (${sql.literal(text("handoff_id"))}),
      handoff_fingerprint UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("handoff_fingerprint"))}
      ),
      controlled_thread_reservation_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("controlled_thread_reservation_id"))}
      ),
      thread_id UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      turn_request_command_id UNIQUE NOT NULL CHECK (
        ${sql.literal(text("turn_request_command_id"))}
      ),
      message_id UNIQUE NOT NULL CHECK (${sql.literal(text("message_id"))}),
      provider_instance_id NOT NULL CHECK (${sql.literal(text("provider_instance_id"))}),
      state NOT NULL CHECK (
        ${sql.literal(text("state"))}
        AND state IN (
          'pending', 'turn-accepted', 'claimed', 'delivery-attempted',
          'provider-started',
          'interrupt-requested',
          'retry-wait', 'ambiguous', 'completed', 'failed', 'interrupted'
        )
      ),
      revision NOT NULL CHECK (
        typeof(revision) = 'integer' AND revision >= 0
      ),
      claim_owner_id CHECK (
        claim_owner_id IS NULL OR ${sql.literal(text("claim_owner_id"))}
      ),
      claim_generation NOT NULL CHECK (
        typeof(claim_generation) = 'integer' AND claim_generation >= 0
      ),
      claim_expires_at CHECK (
        claim_expires_at IS NULL OR ${sql.literal(timestamp("claim_expires_at"))}
      ),
      attempt_count NOT NULL CHECK (
        typeof(attempt_count) = 'integer' AND attempt_count >= 0
      ),
      next_attempt_at CHECK (
        next_attempt_at IS NULL OR ${sql.literal(timestamp("next_attempt_at"))}
      ),
      planning_deadline_at NOT NULL CHECK (
        ${sql.literal(timestamp("planning_deadline_at"))}
      ),
      provider_turn_id CHECK (
        provider_turn_id IS NULL OR ${sql.literal(text("provider_turn_id"))}
      ),
      provider_accepted_at CHECK (
        provider_accepted_at IS NULL OR ${sql.literal(timestamp("provider_accepted_at"))}
      ),
      terminal_at CHECK (
        terminal_at IS NULL OR ${sql.literal(timestamp("terminal_at"))}
      ),
      last_error_code CHECK (
        last_error_code IS NULL
        OR (
          ${sql.literal(text("last_error_code"))}
          AND last_error_code IN (
            'transient-not-accepted', 'provider-timeout', 'provider-quota',
            'provider-authority-conflict', 'provider-acceptance-ambiguous',
            'provider-aborted', 'provider-defect', 'planning-deadline',
            'session-incompatible'
          )
        )
      ),
      interrupt_requested NOT NULL CHECK (
        typeof(interrupt_requested) = 'integer'
        AND interrupt_requested IN (0, 1)
      ),
      updated_at NOT NULL CHECK (${sql.literal(timestamp("updated_at"))}),
      CHECK (
        (
          state IN ('pending', 'turn-accepted', 'provider-started', 'interrupt-requested')
          AND claim_owner_id IS NULL AND claim_expires_at IS NULL
          AND next_attempt_at IS NULL AND terminal_at IS NULL
        )
        OR (
          state IN ('claimed', 'delivery-attempted')
          AND claim_owner_id IS NOT NULL AND claim_generation >= 1
          AND claim_expires_at IS NOT NULL
          AND next_attempt_at IS NULL AND terminal_at IS NULL
        )
        OR (
          state = 'retry-wait'
          AND claim_owner_id IS NULL AND claim_expires_at IS NULL
          AND next_attempt_at IS NOT NULL AND terminal_at IS NULL
          AND last_error_code IS NOT NULL
        )
        OR (
          state = 'ambiguous'
          AND claim_owner_id IS NULL AND claim_expires_at IS NULL
          AND next_attempt_at IS NULL AND terminal_at IS NOT NULL
          AND last_error_code = 'provider-acceptance-ambiguous'
        )
        OR (
          state IN ('completed', 'failed', 'interrupted')
          AND claim_owner_id IS NULL AND claim_expires_at IS NULL
          AND next_attempt_at IS NULL AND terminal_at IS NOT NULL
        )
      ),
      CHECK (
        state IN (
          'provider-started', 'interrupt-requested', 'ambiguous',
          'completed', 'failed', 'interrupted'
        )
        OR provider_turn_id IS NULL
      ),
      CHECK (
        (provider_turn_id IS NULL AND provider_accepted_at IS NULL)
        OR (provider_turn_id IS NOT NULL AND provider_accepted_at IS NOT NULL)
      ),
      FOREIGN KEY (
        handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id
      ) REFERENCES agent_control_initial_planning_handoff_accepted(
        handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_delivery_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE INDEX idx_agent_control_initial_planning_delivery_recovery
    ON agent_control_initial_planning_deliveries(
      state, next_attempt_at, claim_expires_at, planning_deadline_at
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_session_evidence (
      provider_delivery_id PRIMARY KEY CHECK (${sql.literal(text("provider_delivery_id"))}),
      thread_id UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      provider_instance_id NOT NULL CHECK (${sql.literal(text("provider_instance_id"))}),
      runtime_mode NOT NULL CHECK (
        ${sql.literal(text("runtime_mode"))}
        AND runtime_mode IN ('approval-required', 'full-access')
      ),
      cwd NOT NULL CHECK (${sql.literal(text("cwd"))}),
      model_selection_json NOT NULL CHECK (${sql.literal(text("model_selection_json"))}),
      model_selection_fingerprint NOT NULL CHECK (
        ${sql.literal(sha256("model_selection_fingerprint"))}
      ),
      session_created_at NOT NULL CHECK (${sql.literal(timestamp("session_created_at"))}),
      resume_cursor_json NOT NULL CHECK (
        ${sql.literal(text("resume_cursor_json"))} AND json_valid(resume_cursor_json) = 1
      ),
      recorded_at NOT NULL CHECK (${sql.literal(timestamp("recorded_at"))}),
      FOREIGN KEY (provider_delivery_id)
      REFERENCES agent_control_initial_planning_deliveries(provider_delivery_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE TRIGGER agent_control_initial_planning_session_evidence_validate
    BEFORE INSERT ON agent_control_initial_planning_session_evidence
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_initial_planning_deliveries delivery
      JOIN agent_control_initial_planning_handoff_intents intent
        ON intent.provider_delivery_id IS delivery.provider_delivery_id
      WHERE delivery.provider_delivery_id IS NEW.provider_delivery_id
        AND delivery.thread_id IS NEW.thread_id
        AND delivery.provider_instance_id IS NEW.provider_instance_id
        AND intent.runtime_mode IS NEW.runtime_mode
        AND intent.worktree_path IS NEW.cwd
        AND json(intent.model_selection_json) IS json(NEW.model_selection_json)
    )
    BEGIN
      SELECT RAISE(ABORT, 'initial planning session evidence is inconsistent');
    END
  `;

  for (const table of [
    "agent_control_initial_planning_handoff_intents",
    "agent_control_initial_planning_handoff_receipts",
    "agent_control_initial_planning_handoff_accepted",
  ] as const) {
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_reject_legacy
       BEFORE INSERT ON ${table}
       WHEN EXISTS (
         SELECT 1
         FROM agent_control_initial_planning_legacy_materializations legacy
         WHERE legacy.coordinator_command_id IS NEW.coordinator_command_id
            OR legacy.controlled_thread_reservation_id IS
              NEW.controlled_thread_reservation_id
            OR legacy.thread_id IS NEW.thread_id
       )
       BEGIN
         SELECT RAISE(
           ABORT,
           'legacy controlled thread materialization cannot acquire planning handoff evidence'
         );
       END`,
    ).unprepared;
  }

  yield* sql`
    CREATE TRIGGER agent_control_initial_planning_handoff_intent_validate
    BEFORE INSERT ON agent_control_initial_planning_handoff_intents
    WHEN NOT COALESCE((
      ${sql.literal(modelSelectionPredicate)}
      AND instr(NEW.prompt_text, NEW.worktree_path) = 0
      AND instr(NEW.prompt_text, NEW.lease_holder_id) = 0
      AND instr(NEW.prompt_text, NEW.lease_id) = 0
      AND instr(NEW.prompt_text, NEW.controlled_thread_reservation_id) = 0
      AND instr(NEW.prompt_text, NEW.thread_id) = 0
      AND instr(NEW.prompt_text, NEW.handoff_id) = 0
      AND instr(NEW.prompt_text, NEW.turn_request_command_id) = 0
      AND instr(NEW.prompt_text, NEW.message_id) = 0
      AND instr(NEW.prompt_text, NEW.provider_delivery_id) = 0
      AND EXISTS (
        SELECT 1
        FROM agent_control_controlled_thread_materialization_intents materialization
        JOIN agent_control_controlled_thread_materialization_receipts receipt
          ON receipt.coordinator_command_id IS materialization.coordinator_command_id
        JOIN agent_control_controlled_thread_reservation_states reservation
          ON reservation.controlled_thread_reservation_id IS
            materialization.controlled_thread_reservation_id
         AND reservation.status IS 'bound'
         AND reservation.revision IS 3
        JOIN projection_threads thread
          ON thread.thread_id IS materialization.thread_id
         AND thread.project_id IS materialization.project_id
         AND thread.runtime_mode IS materialization.runtime_mode
         AND thread.interaction_mode IS 'plan'
         AND thread.worktree_path IS materialization.worktree_path
         AND json(thread.model_selection_json) IS
           json(materialization.model_selection_json)
        WHERE materialization.coordinator_command_id IS NEW.coordinator_command_id
          AND materialization.coordinator_command_fingerprint IS
            NEW.coordinator_command_fingerprint
          AND materialization.materialization_command_id IS
            NEW.materialization_command_id
          AND materialization.materialization_command_fingerprint IS
            NEW.materialization_command_fingerprint
          AND materialization.project_id IS NEW.project_id
          AND materialization.controlled_thread_reservation_id IS
            NEW.controlled_thread_reservation_id
          AND materialization.thread_id IS NEW.thread_id
          AND materialization.task_id IS NEW.task_id
          AND materialization.task_revision IS NEW.task_revision
          AND materialization.github_intake_sequence IS NEW.github_intake_sequence
          AND materialization.source_identity_fingerprint IS
            NEW.source_identity_fingerprint
          AND materialization.stage_run_id IS NEW.stage_run_id
          AND materialization.attempt_id IS NEW.attempt_id
          AND materialization.role_id IS NEW.role_id
          AND materialization.stage_kind IS NEW.stage_kind
          AND materialization.stage_ordinal IS NEW.stage_ordinal
          AND materialization.attempt_ordinal IS NEW.attempt_ordinal
          AND materialization.lease_id IS NEW.lease_id
          AND materialization.lease_holder_id IS NEW.lease_holder_id
          AND materialization.fence_token IS NEW.fence_token
          AND materialization.worktree_reservation_id IS
            NEW.worktree_reservation_id
          AND materialization.worktree_path IS NEW.worktree_path
          AND materialization.runtime_mode IS NEW.runtime_mode
          AND json(materialization.model_selection_json) IS
            json(NEW.model_selection_json)
          AND receipt.status IS 'accepted'
          AND receipt.controlled_thread_reservation_id IS
            NEW.controlled_thread_reservation_id
          AND receipt.thread_id IS NEW.thread_id
      )
    ), 0)
    BEGIN
      SELECT RAISE(
        ABORT,
        'initial planning handoff intent is noncanonical or incompletely bound'
      );
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_initial_planning_handoff_receipt_validate
    BEFORE INSERT ON agent_control_initial_planning_handoff_receipts
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_initial_planning_handoff_intents intent
      WHERE intent.handoff_id IS NEW.handoff_id
        AND intent.handoff_fingerprint IS NEW.handoff_fingerprint
        AND intent.coordinator_command_id IS NEW.coordinator_command_id
        AND intent.coordinator_command_fingerprint IS
          NEW.coordinator_command_fingerprint
        AND intent.controlled_thread_reservation_id IS
          NEW.controlled_thread_reservation_id
        AND intent.thread_id IS NEW.thread_id
        AND intent.turn_request_command_id IS NEW.turn_request_command_id
        AND intent.message_id IS NEW.message_id
        AND intent.provider_delivery_id IS NEW.provider_delivery_id
        AND intent.created_at IS NEW.accepted_at
        AND NEW.status IS 'accepted'
        AND NEW.accepted_marker_handoff_id IS NEW.handoff_id
    )
    BEGIN
      SELECT RAISE(ABORT, 'initial planning handoff receipt is inconsistent');
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_initial_planning_handoff_accepted_validate
    BEFORE INSERT ON agent_control_initial_planning_handoff_accepted
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_initial_planning_handoff_intents intent
      JOIN agent_control_initial_planning_handoff_receipts receipt
        ON receipt.handoff_id IS intent.handoff_id
       AND receipt.handoff_fingerprint IS intent.handoff_fingerprint
       AND receipt.coordinator_command_id IS intent.coordinator_command_id
       AND receipt.coordinator_command_fingerprint IS
         intent.coordinator_command_fingerprint
       AND receipt.controlled_thread_reservation_id IS
         intent.controlled_thread_reservation_id
       AND receipt.thread_id IS intent.thread_id
       AND receipt.turn_request_command_id IS intent.turn_request_command_id
       AND receipt.message_id IS intent.message_id
       AND receipt.provider_delivery_id IS intent.provider_delivery_id
       AND receipt.accepted_at IS intent.created_at
       AND receipt.status IS 'accepted'
      WHERE intent.handoff_id IS NEW.handoff_id
        AND intent.handoff_fingerprint IS NEW.handoff_fingerprint
        AND intent.coordinator_command_id IS NEW.coordinator_command_id
        AND intent.coordinator_command_fingerprint IS
          NEW.coordinator_command_fingerprint
        AND intent.controlled_thread_reservation_id IS
          NEW.controlled_thread_reservation_id
        AND intent.thread_id IS NEW.thread_id
        AND intent.turn_request_command_id IS NEW.turn_request_command_id
        AND intent.message_id IS NEW.message_id
        AND intent.provider_delivery_id IS NEW.provider_delivery_id
        AND intent.created_at IS NEW.accepted_at
        AND intent.accepted_marker_handoff_id IS NEW.handoff_id
        AND receipt.accepted_marker_handoff_id IS NEW.handoff_id
    )
    BEGIN
      SELECT RAISE(ABORT, 'initial planning accepted handoff evidence is incomplete');
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_initial_planning_delivery_insert_validate
    BEFORE INSERT ON agent_control_initial_planning_deliveries
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_initial_planning_handoff_intents intent
      JOIN agent_control_initial_planning_handoff_accepted accepted
        ON accepted.handoff_id IS intent.handoff_id
       AND accepted.handoff_fingerprint IS intent.handoff_fingerprint
       AND accepted.provider_delivery_id IS intent.provider_delivery_id
      WHERE intent.handoff_id IS NEW.handoff_id
        AND intent.handoff_fingerprint IS NEW.handoff_fingerprint
        AND intent.controlled_thread_reservation_id IS
          NEW.controlled_thread_reservation_id
        AND intent.thread_id IS NEW.thread_id
        AND intent.turn_request_command_id IS NEW.turn_request_command_id
        AND intent.message_id IS NEW.message_id
        AND intent.provider_delivery_id IS NEW.provider_delivery_id
        AND intent.provider_instance_id IS NEW.provider_instance_id
        AND intent.planning_deadline_at IS NEW.planning_deadline_at
        AND NEW.state IS 'pending'
        AND NEW.revision IS 0
        AND NEW.claim_generation IS 0
        AND NEW.attempt_count IS 0
        AND NEW.interrupt_requested IS 0
        AND NEW.updated_at IS intent.created_at
    )
    BEGIN
      SELECT RAISE(ABORT, 'initial planning delivery is inconsistent');
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_initial_planning_turn_accepted_validate
    BEFORE INSERT ON agent_control_initial_planning_turn_accepted
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_initial_planning_handoff_intents intent
      JOIN agent_control_initial_planning_handoff_accepted accepted
        ON accepted.handoff_id IS intent.handoff_id
      JOIN orchestration_command_receipts command_receipt
        ON command_receipt.command_id IS intent.turn_request_command_id
       AND command_receipt.authority IS 'agent-control'
       AND command_receipt.aggregate_kind IS 'thread'
       AND command_receipt.aggregate_id IS intent.thread_id
       AND command_receipt.status IS 'accepted'
      JOIN orchestration_events message_event
        ON message_event.event_id IS NEW.message_event_id
       AND message_event.sequence IS NEW.message_event_sequence
       AND message_event.command_id IS intent.turn_request_command_id
       AND message_event.stream_id IS intent.thread_id
       AND message_event.aggregate_kind IS 'thread'
       AND message_event.event_type IS 'thread.message-sent'
      JOIN orchestration_events turn_event
        ON turn_event.event_id IS NEW.turn_request_event_id
       AND turn_event.sequence IS NEW.turn_request_event_sequence
       AND turn_event.command_id IS intent.turn_request_command_id
       AND turn_event.stream_id IS intent.thread_id
       AND turn_event.aggregate_kind IS 'thread'
       AND turn_event.event_type IS 'thread.turn-start-requested'
      JOIN projection_thread_messages message
        ON message.message_id IS intent.message_id
       AND message.thread_id IS intent.thread_id
       AND message.role IS 'user'
       AND message.text IS intent.prompt_text
      JOIN projection_turns pending_turn
        ON pending_turn.thread_id IS intent.thread_id
       AND pending_turn.turn_id IS NULL
       AND pending_turn.pending_message_id IS intent.message_id
       AND pending_turn.state IS 'pending'
      WHERE intent.handoff_id IS NEW.handoff_id
        AND intent.handoff_fingerprint IS NEW.handoff_fingerprint
        AND intent.controlled_thread_reservation_id IS
          NEW.controlled_thread_reservation_id
        AND intent.thread_id IS NEW.thread_id
        AND intent.turn_request_command_id IS NEW.turn_request_command_id
        AND intent.message_id IS NEW.message_id
        AND NEW.accepted_at IS command_receipt.accepted_at
        AND NEW.receipt_authority IS command_receipt.authority
        AND command_receipt.result_sequence IS NEW.turn_request_event_sequence
        AND NEW.message_event_sequence < NEW.turn_request_event_sequence
        AND json_extract(message_event.payload_json, '$.messageId') IS intent.message_id
        AND json_extract(message_event.payload_json, '$.text') IS intent.prompt_text
        AND json_extract(turn_event.payload_json, '$.messageId') IS intent.message_id
        AND (
          SELECT count(*)
          FROM orchestration_events candidate
          WHERE candidate.command_id IS intent.turn_request_command_id
        ) IS 2
    )
    BEGIN
      SELECT RAISE(ABORT, 'initial planning turn acceptance is incomplete');
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_initial_planning_materialization_requires_handoff
    BEFORE INSERT ON agent_control_controlled_thread_materialization_accepted
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_initial_planning_legacy_materializations legacy
      WHERE legacy.coordinator_command_id IS NEW.coordinator_command_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM agent_control_initial_planning_handoff_intents intent
      JOIN agent_control_initial_planning_handoff_receipts receipt
        ON receipt.handoff_id IS intent.handoff_id
      JOIN agent_control_initial_planning_handoff_accepted accepted
        ON accepted.handoff_id IS intent.handoff_id
      JOIN agent_control_initial_planning_deliveries delivery
        ON delivery.handoff_id IS intent.handoff_id
      WHERE intent.coordinator_command_id IS NEW.coordinator_command_id
        AND intent.coordinator_command_fingerprint IS
          NEW.coordinator_command_fingerprint
        AND intent.controlled_thread_reservation_id IS
          NEW.controlled_thread_reservation_id
        AND intent.thread_id IS NEW.thread_id
        AND intent.materialization_command_id IS NEW.materialization_command_id
        AND intent.materialization_command_fingerprint IS
          NEW.materialization_command_fingerprint
        AND receipt.handoff_fingerprint IS intent.handoff_fingerprint
        AND accepted.handoff_fingerprint IS intent.handoff_fingerprint
        AND delivery.handoff_fingerprint IS intent.handoff_fingerprint
        AND delivery.state IS 'pending'
        AND delivery.revision IS 0
    )
    BEGIN
      SELECT RAISE(
        ABORT,
        'post-cutover controlled thread materialization requires initial planning handoff'
      );
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_initial_planning_delivery_transition_validate
    BEFORE UPDATE ON agent_control_initial_planning_deliveries
    WHEN
      OLD.provider_delivery_id IS NOT NEW.provider_delivery_id
      OR OLD.handoff_id IS NOT NEW.handoff_id
      OR OLD.handoff_fingerprint IS NOT NEW.handoff_fingerprint
      OR OLD.controlled_thread_reservation_id IS NOT
        NEW.controlled_thread_reservation_id
      OR OLD.thread_id IS NOT NEW.thread_id
      OR OLD.turn_request_command_id IS NOT NEW.turn_request_command_id
      OR OLD.message_id IS NOT NEW.message_id
      OR OLD.provider_instance_id IS NOT NEW.provider_instance_id
      OR OLD.planning_deadline_at IS NOT NEW.planning_deadline_at
      OR NEW.revision IS NOT OLD.revision + 1
      OR NOT (
        (OLD.state = 'pending' AND NEW.state = 'turn-accepted'
          AND NEW.claim_generation = OLD.claim_generation
          AND EXISTS (
            SELECT 1
            FROM agent_control_initial_planning_turn_accepted turn_accepted
            WHERE turn_accepted.handoff_id IS NEW.handoff_id
              AND turn_accepted.handoff_fingerprint IS NEW.handoff_fingerprint
              AND turn_accepted.controlled_thread_reservation_id IS
                NEW.controlled_thread_reservation_id
              AND turn_accepted.thread_id IS NEW.thread_id
              AND turn_accepted.turn_request_command_id IS
                NEW.turn_request_command_id
              AND turn_accepted.message_id IS NEW.message_id
          ))
        OR
        (OLD.state IN ('pending', 'turn-accepted', 'retry-wait', 'claimed')
          AND NEW.state IN ('failed', 'interrupted')
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count)
        OR
        (OLD.state IN ('turn-accepted', 'retry-wait')
          AND NEW.state = 'claimed'
          AND NEW.claim_generation = OLD.claim_generation + 1
          AND NEW.attempt_count = OLD.attempt_count + 1
          AND EXISTS (
            SELECT 1
            FROM agent_control_initial_planning_turn_accepted turn_accepted
            WHERE turn_accepted.handoff_id IS NEW.handoff_id
              AND turn_accepted.handoff_fingerprint IS NEW.handoff_fingerprint
              AND turn_accepted.controlled_thread_reservation_id IS
                NEW.controlled_thread_reservation_id
              AND turn_accepted.thread_id IS NEW.thread_id
              AND turn_accepted.turn_request_command_id IS
                NEW.turn_request_command_id
              AND turn_accepted.message_id IS NEW.message_id
          ))
        OR
        (OLD.state = 'claimed' AND NEW.state = 'claimed'
          AND NEW.claim_generation = OLD.claim_generation + 1
          AND NEW.attempt_count = OLD.attempt_count + 1)
        OR
        (OLD.state = 'claimed' AND NEW.state = 'retry-wait'
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count)
        OR
        (OLD.state = 'delivery-attempted' AND NEW.state = 'retry-wait'
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count)
        OR
        (OLD.state = 'claimed'
          AND NEW.state = 'delivery-attempted'
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count)
        OR
        (OLD.state = 'delivery-attempted'
          AND NEW.state IN (
            'provider-started', 'ambiguous', 'failed', 'interrupted'
          )
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count)
        OR
        (OLD.state = 'provider-started'
          AND NEW.state IN ('completed', 'failed', 'interrupted', 'ambiguous')
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count)
        OR
        (OLD.state = 'provider-started' AND NEW.state = 'interrupt-requested'
          AND OLD.interrupt_requested = 0 AND NEW.interrupt_requested = 1
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count)
        OR
        (OLD.state = 'interrupt-requested'
          AND NEW.state IN ('completed', 'failed', 'interrupted', 'ambiguous')
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count)
        OR
        (OLD.state = 'ambiguous'
          AND NEW.state IN ('completed', 'failed', 'interrupted')
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count)
      )
      OR (
        OLD.state IN ('completed', 'failed', 'interrupted')
      )
    BEGIN
      SELECT RAISE(ABORT, 'invalid initial planning delivery transition');
    END
  `;

  for (const [table, noun] of [
    ["agent_control_initial_planning_legacy_materializations", "initial planning legacy cutover"],
    ["agent_control_initial_planning_handoff_intents", "initial planning handoff intent"],
    ["agent_control_initial_planning_handoff_receipts", "initial planning handoff receipt"],
    ["agent_control_initial_planning_handoff_accepted", "initial planning accepted handoff"],
    ["agent_control_initial_planning_turn_accepted", "initial planning turn acceptance"],
    ["agent_control_initial_planning_session_evidence", "initial planning session evidence"],
  ] as const) {
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_update
       BEFORE UPDATE ON ${table}
       BEGIN SELECT RAISE(ABORT, '${noun} is immutable'); END`,
    ).unprepared;
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_delete
       BEFORE DELETE ON ${table}
       BEGIN SELECT RAISE(ABORT, '${noun} is immutable'); END`,
    ).unprepared;
  }

  yield* sql`
    CREATE TRIGGER agent_control_initial_planning_legacy_no_insert
    BEFORE INSERT ON agent_control_initial_planning_legacy_materializations
    BEGIN
      SELECT RAISE(ABORT, 'initial planning legacy cutover is immutable');
    END
  `;
});
