import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const canonicalUtf8 = (column: string) => `
  instr(${column}, char(0)) = 0
  AND NOT EXISTS (
    WITH RECURSIVE utf8(value) AS (
      SELECT ${column}
      UNION ALL
      SELECT substr(value, 2) FROM utf8 WHERE length(value) > 0
    )
    SELECT 1 FROM utf8
    WHERE length(value) > 0
      AND hex(CAST(substr(value, 1, 1) AS BLOB)) !=
        hex(CAST(char(unicode(value)) AS BLOB))
  )
`;
const text = (column: string) => `typeof(${column}) = 'text' AND length(${column}) > 0`;
const utf8Text = (column: string) => `${text(column)} AND ${canonicalUtf8(column)}`;
const integer = (column: string) => `typeof(${column}) = 'integer'`;
const sha256 = (column: string) =>
  `${text(column)} AND length(${column}) = 64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;
const timestamp = (column: string) => `
  ${text(column)}
  AND length(${column}) = 24
  AND ${column} GLOB
    '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
  AND substr(${column}, 12, 2) BETWEEN '00' AND '23'
  AND strftime('%Y-%m-%dT%H:%M:%fZ', ${column}) = ${column}
`;
const canonicalJson = (column: string) =>
  `${text(column)} AND json_valid(${column}) = 1 AND json(${column}) = ${column}`;
const quote = (identifier: string) => `"${identifier.replaceAll('"', '""')}"`;

interface SchemaObject {
  readonly name: string;
  readonly sql: string;
}

const deliveryStoragePredicate = (row = "NEW") =>
  [
    ...[
      "provider_delivery_id",
      "handoff_id",
      "admission_marker_id",
      "materialization_evidence_id",
      "controlled_thread_reservation_id",
      "thread_id",
      "stage_run_id",
      "attempt_id",
      "lease_id",
      "lease_holder_id",
      "provider_instance_id",
      "runtime_mode",
      "turn_request_command_id",
      "message_id",
      "planning_thread_id",
      "plan_id",
      "state",
    ].map((column) => utf8Text(`${row}.${column}`)),
    ...["handoff_fingerprint", "model_selection_fingerprint"].map((column) =>
      sha256(`${row}.${column}`),
    ),
    ...["fence_token", "revision", "claim_generation", "attempt_count", "interrupt_requested"].map(
      (column) => integer(`${row}.${column}`),
    ),
    ...["claim_owner_id", "provider_turn_id", "last_error_code"].map(
      (column) => `(${row}.${column} IS NULL OR (${utf8Text(`${row}.${column}`)}))`,
    ),
    ...[
      "claim_expires_at",
      "next_attempt_at",
      "provider_accepted_at",
      "provider_session_created_at",
      "terminal_at",
    ].map((column) => `(${row}.${column} IS NULL OR (${timestamp(`${row}.${column}`)}))`),
    `(${row}.provider_resume_cursor_json IS NULL OR (${canonicalJson(
      `${row}.provider_resume_cursor_json`,
    )}))`,
    `(${row}.terminal_event_id IS NULL OR (
      ${utf8Text(`${row}.terminal_event_id`)}
      AND length(CAST(${row}.terminal_event_id AS BLOB)) BETWEEN 1 AND 1024
    ))`,
    `(${row}.terminal_event_type IS NULL OR (${utf8Text(`${row}.terminal_event_type`)}))`,
    `(${row}.terminal_provider_state IS NULL OR (${utf8Text(`${row}.terminal_provider_state`)}))`,
    `(${row}.terminal_observation_digest IS NULL OR (${sha256(
      `${row}.terminal_observation_digest`,
    )}))`,
    timestamp(`${row}.updated_at`),
  ].join(" AND ");

const same = (column: string) => `NEW.${column} IS OLD.${column}`;
const immutableIdentity = [
  "provider_delivery_id",
  "handoff_id",
  "handoff_fingerprint",
  "admission_marker_id",
  "materialization_evidence_id",
  "controlled_thread_reservation_id",
  "thread_id",
  "stage_run_id",
  "attempt_id",
  "lease_id",
  "lease_holder_id",
  "fence_token",
  "provider_instance_id",
  "runtime_mode",
  "model_selection_fingerprint",
  "turn_request_command_id",
  "message_id",
  "planning_thread_id",
  "plan_id",
] as const;
const sameIdentity = immutableIdentity.map(same).join(" AND ");
const sameProviderBinding = [
  "provider_turn_id",
  "provider_accepted_at",
  "provider_session_created_at",
  "provider_resume_cursor_json",
]
  .map(same)
  .join(" AND ");
const sameTerminalObservation = [
  "terminal_at",
  "terminal_event_id",
  "terminal_event_type",
  "terminal_provider_state",
  "terminal_observation_digest",
]
  .map(same)
  .join(" AND ");

const installDeliveryTriggers = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_deliveries_storage_validate
    BEFORE INSERT ON agent_control_verification_deliveries
    WHEN NOT COALESCE((${deliveryStoragePredicate()}), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification evidence storage'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_deliveries_update_storage_validate
    BEFORE UPDATE ON agent_control_verification_deliveries
    WHEN NOT COALESCE((${deliveryStoragePredicate()}), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification evidence storage'); END
  `).unprepared;
  yield* sql`
    CREATE TRIGGER agent_control_verification_deliveries_no_delete
    BEFORE DELETE ON agent_control_verification_deliveries
    BEGIN SELECT RAISE(ABORT, 'verification delivery is immutable'); END
  `;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_delivery_transition_validate
    BEFORE UPDATE ON agent_control_verification_deliveries
    WHEN NOT COALESCE((
      ${sameIdentity}
      AND NEW.interrupt_requested IS OLD.interrupt_requested
      AND NEW.revision = OLD.revision + 1
      AND (
        (OLD.state = 'pending' AND NEW.state = 'turn-accepted'
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count
          AND ${sameProviderBinding}
          AND ${sameTerminalObservation}
          AND NEW.last_error_code IS OLD.last_error_code
          AND EXISTS (
            SELECT 1 FROM agent_control_verification_turn_accepted accepted
            WHERE accepted.handoff_id = NEW.handoff_id
              AND accepted.turn_request_command_id = NEW.turn_request_command_id
          ))
        OR (OLD.state IN ('turn-accepted', 'retry-wait', 'claimed')
          AND NEW.state = 'claimed'
          AND NEW.claim_generation = OLD.claim_generation + 1
          AND NEW.attempt_count = OLD.attempt_count + 1
          AND ${sameProviderBinding}
          AND ${sameTerminalObservation}
          AND NEW.last_error_code IS OLD.last_error_code)
        OR (OLD.state = 'claimed' AND NEW.state = 'delivery-attempted'
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count
          AND NEW.provider_turn_id IS OLD.provider_turn_id
          AND NEW.provider_accepted_at IS OLD.provider_accepted_at
          AND ${sameTerminalObservation}
          AND NEW.last_error_code IS OLD.last_error_code
          AND EXISTS (
            SELECT 1 FROM agent_control_verification_session_evidence session
            JOIN agent_control_verification_delivery_attestations attestation
              ON attestation.provider_delivery_id = session.provider_delivery_id
            WHERE session.provider_delivery_id = NEW.provider_delivery_id
              AND session.provider_instance_id = NEW.provider_instance_id
              AND session.runtime_mode = NEW.runtime_mode
              AND session.model_selection_fingerprint = NEW.model_selection_fingerprint
              AND attestation.provider_instance_id = NEW.provider_instance_id
              AND attestation.model_selection_fingerprint = NEW.model_selection_fingerprint
          ))
        OR (OLD.state = 'claimed' AND NEW.state = 'retry-wait'
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count
          AND ${sameProviderBinding}
          AND ${sameTerminalObservation})
        OR (OLD.state = 'delivery-attempted' AND NEW.state = 'provider-started'
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count
          AND NEW.provider_session_created_at IS OLD.provider_session_created_at
          AND NEW.provider_resume_cursor_json IS OLD.provider_resume_cursor_json
          AND ${sameTerminalObservation})
        OR (OLD.state = 'delivery-attempted' AND NEW.state = 'ambiguous'
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count
          AND ${sameProviderBinding}
          AND NEW.terminal_event_id IS OLD.terminal_event_id
          AND NEW.terminal_event_type IS OLD.terminal_event_type
          AND NEW.terminal_provider_state IS OLD.terminal_provider_state
          AND NEW.terminal_observation_digest IS OLD.terminal_observation_digest)
        OR (OLD.state = 'ambiguous' AND NEW.state = 'provider-started'
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count
          AND NEW.provider_session_created_at IS OLD.provider_session_created_at
          AND NEW.provider_resume_cursor_json IS OLD.provider_resume_cursor_json
          AND NEW.terminal_event_id IS OLD.terminal_event_id
          AND NEW.terminal_event_type IS OLD.terminal_event_type
          AND NEW.terminal_provider_state IS OLD.terminal_provider_state
          AND NEW.terminal_observation_digest IS OLD.terminal_observation_digest)
        OR (OLD.state = 'provider-started'
          AND NEW.state IN ('completed', 'failed', 'interrupted')
          AND NEW.claim_generation = OLD.claim_generation
          AND NEW.attempt_count = OLD.attempt_count
          AND ${sameProviderBinding})
      )
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification delivery transition'); END
  `).unprepared;
});

/** Durable, replay-safe observation of the technical Verification provider-turn terminal. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    SELECT name FROM pragma_table_info('agent_control_verification_deliveries')
    WHERE name = 'terminal_observation_digest'
  `;
  if (columns.length === 1) return;

  const [deliveryTriggers, deliveryIndexes] = yield* Effect.all([
    sql<SchemaObject>`
      SELECT name, sql FROM sqlite_schema
      WHERE type = 'trigger' AND sql IS NOT NULL
        AND (tbl_name = 'agent_control_verification_deliveries'
          OR sql LIKE '%agent_control_verification_deliveries%')
      ORDER BY name
    `,
    sql<SchemaObject>`
      SELECT name, sql FROM sqlite_schema
      WHERE type = 'index' AND tbl_name = 'agent_control_verification_deliveries'
        AND sql IS NOT NULL
      ORDER BY name
    `,
  ]);
  const stageEventTriggers = deliveryTriggers.filter(
    (trigger) => trigger.name === "agent_control_verification_stage_event_validate",
  );
  if (stageEventTriggers.length !== 1) {
    return yield* Effect.die(
      new Error("migration 059 could not capture the Verification stage trigger"),
    );
  }

  yield* sql`PRAGMA defer_foreign_keys = ON`;
  yield* sql`
    CREATE TABLE agent_control_verification_deliveries_rebuild_059 (
      provider_delivery_id TEXT PRIMARY KEY,
      handoff_id TEXT NOT NULL UNIQUE,
      handoff_fingerprint TEXT NOT NULL UNIQUE,
      admission_marker_id TEXT NOT NULL UNIQUE,
      materialization_evidence_id TEXT NOT NULL UNIQUE,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      stage_run_id TEXT NOT NULL UNIQUE,
      attempt_id TEXT NOT NULL UNIQUE,
      lease_id TEXT NOT NULL,
      lease_holder_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
      provider_instance_id TEXT NOT NULL,
      runtime_mode TEXT NOT NULL CHECK (runtime_mode = 'approval-required'),
      model_selection_fingerprint TEXT NOT NULL,
      turn_request_command_id TEXT NOT NULL UNIQUE,
      message_id TEXT NOT NULL UNIQUE,
      planning_thread_id TEXT NOT NULL,
      plan_id TEXT NOT NULL,
      state TEXT NOT NULL CHECK (state IN (
        'pending', 'turn-accepted', 'claimed', 'delivery-attempted',
        'provider-started', 'completed', 'failed', 'interrupted',
        'retry-wait', 'ambiguous'
      )),
      revision INTEGER NOT NULL CHECK (revision >= 0),
      claim_owner_id TEXT,
      claim_generation INTEGER NOT NULL CHECK (claim_generation >= 0),
      claim_expires_at TEXT,
      attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
      next_attempt_at TEXT,
      provider_turn_id TEXT,
      provider_accepted_at TEXT,
      provider_session_created_at TEXT,
      provider_resume_cursor_json TEXT CHECK (
        provider_resume_cursor_json IS NULL OR json_valid(provider_resume_cursor_json) = 1
      ),
      terminal_at TEXT,
      terminal_event_id TEXT,
      terminal_event_type TEXT CHECK (
        terminal_event_type IS NULL OR terminal_event_type IN ('turn.completed', 'turn.aborted')
      ),
      terminal_provider_state TEXT CHECK (
        terminal_provider_state IS NULL OR terminal_provider_state IN (
          'completed', 'failed', 'interrupted', 'cancelled'
        )
      ),
      terminal_observation_digest TEXT,
      last_error_code TEXT CHECK (
        last_error_code IS NULL OR last_error_code IN (
          'provider-quota', 'provider-timeout', 'session-incompatible',
          'transient-not-accepted', 'provider-acceptance-ambiguous',
          'provider-turn-failed', 'provider-turn-aborted',
          'provider-turn-interrupted', 'provider-turn-cancelled'
        )
      ),
      interrupt_requested INTEGER NOT NULL CHECK (interrupt_requested IN (0, 1)),
      updated_at TEXT NOT NULL,
      CHECK (
        (state IN ('pending', 'turn-accepted', 'provider-started')
          AND claim_owner_id IS NULL AND claim_expires_at IS NULL
          AND next_attempt_at IS NULL AND terminal_at IS NULL)
        OR (state IN ('claimed', 'delivery-attempted')
          AND claim_owner_id IS NOT NULL AND claim_generation >= 1
          AND claim_expires_at IS NOT NULL AND next_attempt_at IS NULL
          AND terminal_at IS NULL)
        OR (state = 'retry-wait' AND claim_owner_id IS NULL
          AND claim_expires_at IS NULL AND next_attempt_at IS NOT NULL
          AND terminal_at IS NULL AND last_error_code IS NOT NULL)
        OR (state = 'ambiguous' AND claim_owner_id IS NULL
          AND claim_expires_at IS NULL AND next_attempt_at IS NULL
          AND terminal_at IS NOT NULL
          AND last_error_code = 'provider-acceptance-ambiguous')
        OR (state IN ('completed', 'failed', 'interrupted')
          AND claim_owner_id IS NULL AND claim_expires_at IS NULL
          AND next_attempt_at IS NULL AND terminal_at IS NOT NULL)
      ),
      CHECK (
        (state IN ('provider-started', 'completed', 'failed', 'interrupted')
          AND provider_turn_id IS NOT NULL AND provider_accepted_at IS NOT NULL)
        OR (state NOT IN ('provider-started', 'completed', 'failed', 'interrupted')
          AND provider_turn_id IS NULL AND provider_accepted_at IS NULL)
      ),
      CHECK (
        (state IN ('pending', 'turn-accepted', 'provider-started')
          AND last_error_code IS NULL)
        OR (state IN ('claimed', 'delivery-attempted')
          AND (last_error_code IS NULL OR last_error_code IN (
            'provider-quota', 'provider-timeout', 'session-incompatible',
            'transient-not-accepted'
          )))
        OR (state = 'retry-wait' AND last_error_code IN (
          'provider-quota', 'provider-timeout', 'session-incompatible',
          'transient-not-accepted'
        ))
        OR (state = 'ambiguous' AND last_error_code = 'provider-acceptance-ambiguous')
        OR state IN ('completed', 'failed', 'interrupted')
      ),
      CHECK (
        (state IN ('completed', 'failed', 'interrupted')
          AND terminal_event_id IS NOT NULL AND terminal_event_type IS NOT NULL
          AND terminal_observation_digest IS NOT NULL
          AND terminal_at >= provider_accepted_at AND updated_at = terminal_at)
        OR (state NOT IN ('completed', 'failed', 'interrupted')
          AND terminal_event_id IS NULL AND terminal_event_type IS NULL
          AND terminal_provider_state IS NULL AND terminal_observation_digest IS NULL)
      ),
      CHECK (
        state <> 'completed' OR COALESCE((
          terminal_event_type = 'turn.completed'
          AND terminal_provider_state = 'completed' AND last_error_code IS NULL
        ), 0)
      ),
      CHECK (
        state <> 'failed' OR COALESCE((
          (terminal_event_type = 'turn.completed' AND terminal_provider_state = 'failed'
            AND last_error_code = 'provider-turn-failed')
          OR (terminal_event_type = 'turn.aborted' AND terminal_provider_state IS NULL
            AND last_error_code = 'provider-turn-aborted')
        ), 0)
      ),
      CHECK (
        state <> 'interrupted' OR COALESCE((
          (terminal_event_type = 'turn.completed' AND terminal_provider_state = 'interrupted'
            AND last_error_code = 'provider-turn-interrupted')
          OR (terminal_event_type = 'turn.completed' AND terminal_provider_state = 'cancelled'
            AND last_error_code = 'provider-turn-cancelled')
        ), 0)
      ),
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_verification_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_deliveries_rebuild_059_storage_validate
    BEFORE INSERT ON agent_control_verification_deliveries_rebuild_059
    WHEN NOT COALESCE((${deliveryStoragePredicate()}), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification evidence storage'); END
  `).unprepared;
  yield* sql`
    INSERT INTO agent_control_verification_deliveries_rebuild_059 (
      provider_delivery_id, handoff_id, handoff_fingerprint, admission_marker_id,
      materialization_evidence_id, controlled_thread_reservation_id, thread_id,
      stage_run_id, attempt_id, lease_id, lease_holder_id, fence_token,
      provider_instance_id, runtime_mode, model_selection_fingerprint,
      turn_request_command_id, message_id, planning_thread_id, plan_id,
      state, revision, claim_owner_id, claim_generation, claim_expires_at,
      attempt_count, next_attempt_at, provider_turn_id, provider_accepted_at,
      provider_session_created_at, provider_resume_cursor_json, terminal_at,
      terminal_event_id, terminal_event_type, terminal_provider_state,
      terminal_observation_digest, last_error_code, interrupt_requested, updated_at
    )
    SELECT provider_delivery_id, handoff_id, handoff_fingerprint, admission_marker_id,
      materialization_evidence_id, controlled_thread_reservation_id, thread_id,
      stage_run_id, attempt_id, lease_id, lease_holder_id, fence_token,
      provider_instance_id, runtime_mode, model_selection_fingerprint,
      turn_request_command_id, message_id, planning_thread_id, plan_id,
      state, revision, claim_owner_id, claim_generation, claim_expires_at,
      attempt_count, next_attempt_at, provider_turn_id, provider_accepted_at,
      provider_session_created_at, provider_resume_cursor_json, terminal_at,
      NULL, NULL, NULL, NULL, last_error_code, interrupt_requested, updated_at
    FROM agent_control_verification_deliveries
  `;

  for (const trigger of deliveryTriggers) {
    yield* sql.unsafe(`DROP TRIGGER ${quote(trigger.name)}`).unprepared;
  }
  yield* sql`DROP TABLE agent_control_verification_deliveries`;
  yield* sql`
    ALTER TABLE agent_control_verification_deliveries_rebuild_059
    RENAME TO agent_control_verification_deliveries
  `;
  yield* sql`
    DROP TRIGGER agent_control_verification_deliveries_rebuild_059_storage_validate
  `;

  for (const index of deliveryIndexes) yield* sql.unsafe(index.sql).unprepared;
  for (const trigger of deliveryTriggers) {
    if (
      trigger.name === "agent_control_verification_delivery_transition_validate" ||
      trigger.name === "agent_control_verification_deliveries_no_delete" ||
      trigger.name === "agent_control_verification_deliveries_storage_validate" ||
      trigger.name === "agent_control_verification_deliveries_update_storage_validate" ||
      trigger.name === "agent_control_verification_stage_event_validate"
    ) {
      continue;
    }
    yield* sql.unsafe(trigger.sql).unprepared;
  }
  yield* installDeliveryTriggers;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_verification_delivery_terminal_event
    ON agent_control_verification_deliveries(provider_instance_id, terminal_event_id)
    WHERE terminal_event_id IS NOT NULL
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_verification_delivery_terminal_recovery
    ON agent_control_verification_deliveries(state, handoff_id)
  `;

  const originalStageTrigger = stageEventTriggers[0]!.sql;
  const expandedStageTrigger = originalStageTrigger.replace(
    "delivery.state = 'provider-started'",
    "delivery.state IN ('provider-started', 'completed', 'failed', 'interrupted')",
  );
  if (expandedStageTrigger === originalStageTrigger) {
    return yield* Effect.die(
      new Error("migration 059 could not expand the Verification stage delivery guard"),
    );
  }
  yield* sql.unsafe(expandedStageTrigger).unprepared;

  const violations = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
  if (violations.length !== 0) {
    return yield* Effect.die(new Error("migration 059 introduced foreign-key violations"));
  }
});
