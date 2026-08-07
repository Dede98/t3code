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
// Large prompt/task payloads are selected as BLOBs and decoded with
// decodeCanonicalUtf8Bytes by the Verification store before recovery use.
// Keep the recursive SQL validation on short identity and recovery fields.
const uncheckedText = text;
const integer = (column: string, minimum = 0) =>
  `typeof(${column}) = 'integer' AND ${column} >= ${minimum}`;
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
const strictText = text;
const canonicalJson = (column: string) =>
  `${uncheckedText(column)} AND json_valid(${column}) = 1 AND json(${column}) = ${column}`;
const quote = (identifier: string) => `"${identifier.replaceAll('"', '""')}"`;

interface StorageColumns {
  readonly text?: ReadonlyArray<string>;
  readonly uncheckedText?: ReadonlyArray<string>;
  readonly textAllowEmpty?: ReadonlyArray<string>;
  readonly sha256?: ReadonlyArray<string>;
  readonly timestamp?: ReadonlyArray<string>;
  readonly integer?: ReadonlyArray<string>;
  readonly nullableText?: ReadonlyArray<string>;
  readonly nullableTimestamp?: ReadonlyArray<string>;
  readonly json?: ReadonlyArray<string>;
  readonly nullableJson?: ReadonlyArray<string>;
}

const storagePredicate = (columns: StorageColumns, row = "NEW") =>
  [
    ...(columns.text ?? []).map((column) => utf8Text(`${row}.${column}`)),
    ...(columns.uncheckedText ?? []).map((column) => uncheckedText(`${row}.${column}`)),
    ...(columns.textAllowEmpty ?? []).map((column) => `typeof(${row}.${column}) = 'text'`),
    ...(columns.sha256 ?? []).map((column) => sha256(`${row}.${column}`)),
    ...(columns.timestamp ?? []).map((column) => timestamp(`${row}.${column}`)),
    ...(columns.integer ?? []).map((column) => `typeof(${row}.${column}) = 'integer'`),
    ...(columns.nullableText ?? []).map(
      (column) => `(${row}.${column} IS NULL OR (${utf8Text(`${row}.${column}`)}))`,
    ),
    ...(columns.nullableTimestamp ?? []).map(
      (column) => `(${row}.${column} IS NULL OR (${timestamp(`${row}.${column}`)}))`,
    ),
    ...(columns.json ?? []).map(
      (column) =>
        `typeof(${row}.${column}) = 'text' AND json_valid(${row}.${column}) = 1 AND json(${row}.${column}) = ${row}.${column}`,
    ),
    ...(columns.nullableJson ?? []).map(
      (column) =>
        `(${row}.${column} IS NULL OR (typeof(${row}.${column}) = 'text' AND json_valid(${row}.${column}) = 1 AND json(${row}.${column}) = ${row}.${column}))`,
    ),
  ].join(" AND ");

interface SchemaObject {
  readonly name: string;
  readonly sql: string;
}

const captureSchema = (table: string, type: "index" | "trigger") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql<SchemaObject>`
      SELECT name, sql FROM sqlite_schema
      WHERE type = ${type} AND tbl_name = ${table} AND sql IS NOT NULL
      ORDER BY name
    `;
  });

const restoreSchema = (objects: ReadonlyArray<SchemaObject>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const object of objects) yield* sql.unsafe(object.sql).unprepared;
  });

const rebuildAgentControlEvents = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [triggers, indexes, sequenceRows] = yield* Effect.all([
    sql<SchemaObject>`
      SELECT name, sql FROM sqlite_schema
      WHERE type = 'trigger' AND sql IS NOT NULL
        AND (tbl_name = 'agent_control_events' OR sql LIKE '%agent_control_events%')
      ORDER BY name
    `,
    sql<SchemaObject>`
      SELECT name, sql FROM sqlite_schema
      WHERE type = 'index' AND tbl_name = 'agent_control_events' AND sql IS NOT NULL
      ORDER BY name
    `,
    sql<{ readonly seq: number }>`
      SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
    `,
  ]);
  for (const trigger of triggers)
    yield* sql.unsafe(`DROP TRIGGER ${quote(trigger.name)}`).unprepared;
  yield* sql`
    CREATE TABLE agent_control_events_rebuild_058 (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind IN (
        'project-controller', 'github-intake', 'task', 'stage-run',
        'stage-run-lease', 'worktree-reservation', 'controlled-thread-reservation'
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
        OR (aggregate_kind = 'stage-run' AND event_type IN (
          'agentControl.stageRun.prepared', 'agentControl.stageRun.planningStarted',
          'agentControl.stageRun.planningSucceeded', 'agentControl.stageRun.planningFailed',
          'agentControl.stageRun.planningCancelled', 'agentControl.stageRun.implementationStarted',
          'agentControl.stageRun.implementationSucceeded',
          'agentControl.stageRun.implementationFailed',
          'agentControl.stageRun.implementationCancelled',
          'agentControl.stageRun.verificationStarted'
        ))
        OR (aggregate_kind = 'stage-run-lease' AND event_type IN (
          'agentControl.stageRunLease.reserved', 'agentControl.stageRunLease.renewed',
          'agentControl.stageRunLease.releasedBeforeExecution',
          'agentControl.stageRunLease.releasedAfterPlanning',
          'agentControl.stageRunLease.releasedAfterImplementation'
        ))
        OR (aggregate_kind = 'worktree-reservation' AND event_type IN (
          'agentControl.worktree.reserved', 'agentControl.worktree.materializationStarted',
          'agentControl.worktree.ready', 'agentControl.worktree.needsAttention'
        ))
        OR (
          aggregate_kind = 'controlled-thread-reservation'
          AND actor_authority = 'controller'
          AND (
            (stream_version = 1 AND event_type = 'agentControl.controlledThreadReservation.prepared')
            OR (stream_version = 2 AND event_type = 'agentControl.controlledThreadReservation.materializing')
            OR (stream_version = 3 AND event_type = 'agentControl.controlledThreadReservation.bound')
          )
        )
      )
    )
  `;
  yield* sql`INSERT INTO agent_control_events_rebuild_058 SELECT * FROM agent_control_events`;
  yield* sql`DROP TABLE agent_control_events`;
  yield* sql`ALTER TABLE agent_control_events_rebuild_058 RENAME TO agent_control_events`;
  yield* restoreSchema(indexes);
  const sequence = sequenceRows[0]?.seq;
  if (sequence !== undefined) {
    yield* sql`
      DELETE FROM sqlite_sequence
      WHERE name IN ('agent_control_events', 'agent_control_events_rebuild_058')
    `;
    yield* sql`INSERT INTO sqlite_sequence(name, seq) VALUES ('agent_control_events', ${sequence})`;
  }
  yield* restoreSchema(triggers);
});

const rebuildOrchestrationMaterializationIntents = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [triggers, indexes] = yield* Effect.all([
    sql<SchemaObject>`
      SELECT name, sql FROM sqlite_schema
      WHERE type = 'trigger' AND sql IS NOT NULL AND (
        tbl_name = 'orchestration_agent_control_thread_materialization_intents'
        OR sql LIKE '%orchestration_agent_control_thread_materialization_intents%'
      )
      ORDER BY name
    `,
    captureSchema("orchestration_agent_control_thread_materialization_intents", "index"),
  ]);
  for (const trigger of triggers)
    yield* sql.unsafe(`DROP TRIGGER ${quote(trigger.name)}`).unprepared;
  yield* sql`
    CREATE TABLE orchestration_agent_control_thread_materialization_intents_rebuild_058 (
      command_id TEXT PRIMARY KEY,
      command_type TEXT NOT NULL CHECK (command_type = 'thread.agent-control.materialize'),
      authority TEXT NOT NULL CHECK (authority = 'agent-control'),
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind = 'thread'),
      command_fingerprint TEXT NOT NULL CHECK (
        length(command_fingerprint) = 64 AND command_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      controlled_thread_reservation_id TEXT NOT NULL,
      thread_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        length(source_identity_fingerprint) = 64
        AND source_identity_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      stage_run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      role_id TEXT NOT NULL,
      stage_kind TEXT NOT NULL,
      stage_ordinal INTEGER NOT NULL,
      attempt_ordinal INTEGER NOT NULL,
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 1),
      worktree_reservation_id TEXT NOT NULL,
      title TEXT NOT NULL CHECK (length(trim(title)) > 0),
      model_selection_json TEXT NOT NULL CHECK (
        json_valid(model_selection_json) = 1 AND json_type(model_selection_json) = 'object'
      ),
      runtime_mode TEXT NOT NULL CHECK (runtime_mode IN ('approval-required', 'full-access')),
      interaction_mode TEXT NOT NULL CHECK (interaction_mode IN ('default', 'plan')),
      branch TEXT NOT NULL CHECK (length(trim(branch)) > 0),
      worktree_path TEXT NOT NULL CHECK (length(trim(worktree_path)) > 0),
      binding_json TEXT NOT NULL CHECK (
        json_valid(binding_json) = 1 AND json_type(binding_json) = 'object'
      ),
      source_proposed_plan_thread_id TEXT,
      source_proposed_plan_id TEXT,
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
        (
          role_id = 'planning' AND stage_kind = 'planning'
          AND stage_ordinal = 1 AND attempt_ordinal = 1
          AND interaction_mode = 'plan'
          AND source_proposed_plan_thread_id IS NULL
          AND source_proposed_plan_id IS NULL
        ) OR (
          role_id = 'implementer' AND stage_kind = 'implementation'
          AND stage_ordinal = 2 AND attempt_ordinal = 1
          AND interaction_mode = 'default'
          AND ${sql.literal(text("source_proposed_plan_thread_id"))}
          AND ${sql.literal(text("source_proposed_plan_id"))}
        ) OR (
          role_id = 'verifier' AND stage_kind = 'verification'
          AND stage_ordinal = 3 AND attempt_ordinal = 1
          AND runtime_mode = 'approval-required'
          AND interaction_mode = 'default'
          AND ${sql.literal(text("source_proposed_plan_thread_id"))}
          AND ${sql.literal(text("source_proposed_plan_id"))}
        )
      ),
      CHECK (
        (
          receipt_status = 'accepted' AND receipt_error IS NULL
          AND created_event_id IS NOT NULL AND created_event_type = 'thread.created'
          AND created_event_sequence >= 1 AND created_event_stream_version = 1
          AND binding_event_id IS NOT NULL
          AND binding_event_type = 'thread.agent-control-bound'
          AND binding_event_sequence = created_event_sequence + 1
          AND binding_event_stream_version = 2
          AND accepted_receipt_command_id = command_id
          AND receipt_result_sequence = binding_event_sequence
          AND receipt_accepted_at = created_at
        ) OR (
          receipt_status = 'rejected' AND receipt_error IS NOT NULL
          AND created_event_id IS NULL AND created_event_type IS NULL
          AND created_event_sequence IS NULL AND created_event_stream_version IS NULL
          AND binding_event_id IS NULL AND binding_event_type IS NULL
          AND binding_event_sequence IS NULL AND binding_event_stream_version IS NULL
          AND accepted_receipt_command_id IS NULL
        )
      )
    )
  `;
  yield* sql`
    INSERT INTO orchestration_agent_control_thread_materialization_intents_rebuild_058 (
      command_id, command_type, authority, aggregate_kind, command_fingerprint,
      controlled_thread_reservation_id, thread_id, project_id, task_id,
      task_revision, github_intake_sequence, source_identity_fingerprint,
      stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
      attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
      title, model_selection_json, runtime_mode, interaction_mode, branch,
      worktree_path, binding_json, source_proposed_plan_thread_id,
      source_proposed_plan_id, created_event_id, created_event_type,
      created_event_sequence, created_event_stream_version, binding_event_id,
      binding_event_type, binding_event_sequence, binding_event_stream_version,
      accepted_receipt_command_id, receipt_status, receipt_result_sequence,
      receipt_accepted_at, receipt_error, created_at
    )
    SELECT
      command_id, command_type, authority, aggregate_kind, command_fingerprint,
      controlled_thread_reservation_id, thread_id, project_id, task_id,
      task_revision, github_intake_sequence, source_identity_fingerprint,
      stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
      attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
      title, model_selection_json, runtime_mode, interaction_mode, branch,
      worktree_path, binding_json, source_proposed_plan_thread_id,
      source_proposed_plan_id, created_event_id, created_event_type,
      created_event_sequence, created_event_stream_version, binding_event_id,
      binding_event_type, binding_event_sequence, binding_event_stream_version,
      accepted_receipt_command_id, receipt_status, receipt_result_sequence,
      receipt_accepted_at, receipt_error, created_at
    FROM orchestration_agent_control_thread_materialization_intents
  `;
  yield* sql`DROP TABLE orchestration_agent_control_thread_materialization_intents`;
  yield* sql`
    ALTER TABLE orchestration_agent_control_thread_materialization_intents_rebuild_058
    RENAME TO orchestration_agent_control_thread_materialization_intents
  `;
  yield* restoreSchema(indexes);
  yield* restoreSchema(triggers);
});

const rebuildVerificationReservation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const referencedTriggers = yield* sql<SchemaObject>`
    SELECT name, sql FROM sqlite_schema
    WHERE type = 'trigger' AND sql IS NOT NULL AND (
      tbl_name IN (
        'agent_control_verification_thread_stream_catalog',
        'agent_control_verification_thread_reservation_states'
      )
      OR sql LIKE '%agent_control_verification_thread_stream_catalog%'
      OR sql LIKE '%agent_control_verification_thread_reservation_states%'
      OR sql LIKE '%agent_control_controlled_thread_stream_catalog_all%'
      OR sql LIKE '%agent_control_controlled_thread_reservation_states_all%'
    )
    ORDER BY name
  `;
  for (const trigger of referencedTriggers) {
    yield* sql.unsafe(`DROP TRIGGER ${quote(trigger.name)}`).unprepared;
  }
  yield* sql`DROP VIEW agent_control_controlled_thread_stream_catalog_all`;
  yield* sql`DROP VIEW agent_control_controlled_thread_reservation_states_all`;
  yield* sql`
    CREATE TABLE agent_control_verification_thread_stream_catalog_rebuild_058 (
      controlled_thread_reservation_id TEXT NOT NULL,
      event_id TEXT PRIMARY KEY,
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind = 'controlled-thread-reservation'),
      stream_version INTEGER NOT NULL CHECK (stream_version IN (1, 2, 3)),
      command_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      thread_id TEXT NOT NULL CHECK (thread_id LIKE 't3-auto-reserved-thread-%'),
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        length(source_identity_fingerprint) = 64
        AND source_identity_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      stage_run_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      role_id TEXT NOT NULL CHECK (role_id = 'verifier'),
      stage_kind TEXT NOT NULL CHECK (stage_kind = 'verification'),
      stage_ordinal INTEGER NOT NULL CHECK (stage_ordinal = 3),
      attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal = 1),
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
      worktree_reservation_id TEXT NOT NULL,
      prepared_at TEXT NOT NULL,
      coordinator_command_id TEXT,
      coordinator_command_fingerprint TEXT,
      materializing_transition_command_id TEXT,
      materialization_command_id TEXT,
      materialization_command_fingerprint TEXT,
      lease_holder_id TEXT,
      materializing_at TEXT,
      bound_transition_command_id TEXT,
      orchestration_result_sequence INTEGER,
      materialized_at TEXT,
      bound_at TEXT,
      UNIQUE (controlled_thread_reservation_id, stream_version),
      CHECK (
        (stream_version = 1
          AND event_type = 'agentControl.controlledThreadReservation.prepared'
          AND coordinator_command_id IS NULL AND coordinator_command_fingerprint IS NULL
          AND materializing_transition_command_id IS NULL
          AND materialization_command_id IS NULL
          AND materialization_command_fingerprint IS NULL AND lease_holder_id IS NULL
          AND materializing_at IS NULL AND bound_transition_command_id IS NULL
          AND orchestration_result_sequence IS NULL AND materialized_at IS NULL
          AND bound_at IS NULL)
        OR (stream_version = 2
          AND event_type = 'agentControl.controlledThreadReservation.materializing'
          AND coordinator_command_id IS NOT NULL
          AND coordinator_command_fingerprint IS NOT NULL
          AND materializing_transition_command_id IS NOT NULL
          AND materialization_command_id IS NOT NULL
          AND materialization_command_fingerprint IS NOT NULL
          AND lease_holder_id IS NOT NULL AND materializing_at IS NOT NULL
          AND bound_transition_command_id IS NULL
          AND orchestration_result_sequence IS NULL AND materialized_at IS NULL
          AND bound_at IS NULL)
        OR (stream_version = 3
          AND event_type = 'agentControl.controlledThreadReservation.bound'
          AND coordinator_command_id IS NOT NULL
          AND coordinator_command_fingerprint IS NOT NULL
          AND materializing_transition_command_id IS NOT NULL
          AND materialization_command_id IS NOT NULL
          AND materialization_command_fingerprint IS NOT NULL
          AND lease_holder_id IS NOT NULL AND materializing_at IS NOT NULL
          AND bound_transition_command_id IS NOT NULL
          AND orchestration_result_sequence IS NOT NULL
          AND materialized_at IS NOT NULL AND bound_at IS NOT NULL)
      ),
      FOREIGN KEY (event_id, aggregate_kind, controlled_thread_reservation_id,
        stream_version, event_type, command_id)
      REFERENCES agent_control_events(event_id, aggregate_kind, stream_id,
        stream_version, event_type, command_id)
      ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    INSERT INTO agent_control_verification_thread_stream_catalog_rebuild_058
    SELECT controlled_thread_reservation_id, event_id, aggregate_kind, stream_version,
      command_id, event_type, thread_id, project_id, task_id, task_revision,
      github_intake_sequence, source_identity_fingerprint, stage_run_id, attempt_id,
      role_id, stage_kind, stage_ordinal, attempt_ordinal, lease_id, fence_token,
      worktree_reservation_id, prepared_at, NULL, NULL, NULL, NULL, NULL, NULL,
      NULL, NULL, NULL, NULL, NULL
    FROM agent_control_verification_thread_stream_catalog
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_thread_reservation_states_rebuild_058 (
      controlled_thread_reservation_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL UNIQUE,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        length(source_identity_fingerprint) = 64
        AND source_identity_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      stage_run_id TEXT NOT NULL UNIQUE,
      attempt_id TEXT NOT NULL UNIQUE,
      role_id TEXT NOT NULL CHECK (role_id = 'verifier'),
      stage_kind TEXT NOT NULL CHECK (stage_kind = 'verification'),
      stage_ordinal INTEGER NOT NULL CHECK (stage_ordinal = 3),
      attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal = 1),
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
      worktree_reservation_id TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL CHECK (status IN ('prepared', 'materializing', 'bound')),
      revision INTEGER NOT NULL CHECK (
        (status = 'prepared' AND revision = 1)
        OR (status = 'materializing' AND revision = 2)
        OR (status = 'bound' AND revision = 3)
      ),
      last_event_sequence INTEGER NOT NULL UNIQUE CHECK (last_event_sequence >= 1),
      prepared_at TEXT NOT NULL,
      coordinator_command_id TEXT,
      coordinator_command_fingerprint TEXT,
      materializing_transition_command_id TEXT,
      materialization_command_id TEXT,
      materialization_command_fingerprint TEXT,
      lease_holder_id TEXT,
      materializing_at TEXT,
      bound_transition_command_id TEXT,
      orchestration_result_sequence INTEGER,
      materialized_at TEXT,
      bound_at TEXT,
      state_json TEXT NOT NULL CHECK (json_valid(state_json) = 1),
      CHECK (
        (status = 'prepared'
          AND coordinator_command_id IS NULL AND coordinator_command_fingerprint IS NULL
          AND materializing_transition_command_id IS NULL
          AND materialization_command_id IS NULL
          AND materialization_command_fingerprint IS NULL AND lease_holder_id IS NULL
          AND materializing_at IS NULL AND bound_transition_command_id IS NULL
          AND orchestration_result_sequence IS NULL AND materialized_at IS NULL
          AND bound_at IS NULL)
        OR (status = 'materializing'
          AND coordinator_command_id IS NOT NULL
          AND coordinator_command_fingerprint IS NOT NULL
          AND materializing_transition_command_id IS NOT NULL
          AND materialization_command_id IS NOT NULL
          AND materialization_command_fingerprint IS NOT NULL
          AND lease_holder_id IS NOT NULL AND materializing_at IS NOT NULL
          AND bound_transition_command_id IS NULL
          AND orchestration_result_sequence IS NULL AND materialized_at IS NULL
          AND bound_at IS NULL)
        OR (status = 'bound'
          AND coordinator_command_id IS NOT NULL
          AND coordinator_command_fingerprint IS NOT NULL
          AND materializing_transition_command_id IS NOT NULL
          AND materialization_command_id IS NOT NULL
          AND materialization_command_fingerprint IS NOT NULL
          AND lease_holder_id IS NOT NULL AND materializing_at IS NOT NULL
          AND bound_transition_command_id IS NOT NULL
          AND orchestration_result_sequence IS NOT NULL
          AND materialized_at IS NOT NULL AND bound_at IS NOT NULL)
      ),
      FOREIGN KEY (last_event_sequence)
        REFERENCES agent_control_events(sequence) ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    INSERT INTO agent_control_verification_thread_reservation_states_rebuild_058
    SELECT controlled_thread_reservation_id, thread_id, project_id, task_id,
      task_revision, github_intake_sequence, source_identity_fingerprint,
      stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal, attempt_ordinal,
      lease_id, fence_token, worktree_reservation_id, status, revision,
      last_event_sequence, prepared_at, NULL, NULL, NULL, NULL, NULL, NULL,
      NULL, NULL, NULL, NULL, NULL, state_json
    FROM agent_control_verification_thread_reservation_states
  `;
  yield* sql`DROP TABLE agent_control_verification_thread_reservation_states`;
  yield* sql`DROP TABLE agent_control_verification_thread_stream_catalog`;
  yield* sql`
    ALTER TABLE agent_control_verification_thread_stream_catalog_rebuild_058
    RENAME TO agent_control_verification_thread_stream_catalog
  `;
  yield* sql`
    ALTER TABLE agent_control_verification_thread_reservation_states_rebuild_058
    RENAME TO agent_control_verification_thread_reservation_states
  `;
  yield* sql`
    CREATE VIEW agent_control_controlled_thread_stream_catalog_all AS
    SELECT * FROM agent_control_controlled_thread_stream_catalog
    UNION ALL SELECT * FROM agent_control_implementation_thread_stream_catalog
    UNION ALL SELECT * FROM agent_control_verification_thread_stream_catalog
  `;
  yield* sql`
    CREATE VIEW agent_control_controlled_thread_reservation_states_all AS
    SELECT * FROM agent_control_controlled_thread_reservation_states
    UNION ALL SELECT * FROM agent_control_implementation_thread_reservation_states
    UNION ALL SELECT * FROM agent_control_verification_thread_reservation_states
  `;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_catalog_storage_validate
    BEFORE INSERT ON agent_control_verification_thread_stream_catalog
    WHEN NOT COALESCE((
      ${text("NEW.controlled_thread_reservation_id")}
      AND ${text("NEW.event_id")} AND ${text("NEW.command_id")}
      AND ${text("NEW.thread_id")} AND ${text("NEW.project_id")}
      AND ${text("NEW.task_id")} AND ${integer("NEW.task_revision", 1)}
      AND ${integer("NEW.github_intake_sequence", 1)}
      AND ${sha256("NEW.source_identity_fingerprint")}
      AND ${text("NEW.stage_run_id")} AND ${text("NEW.attempt_id")}
      AND ${text("NEW.lease_id")} AND ${integer("NEW.fence_token", 3)}
      AND ${text("NEW.worktree_reservation_id")} AND ${timestamp("NEW.prepared_at")}
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification reservation catalog storage'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_catalog_validate
    BEFORE INSERT ON agent_control_verification_thread_stream_catalog
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_stage_run_states stage
      JOIN agent_control_stage_run_lease_states lease ON lease.lease_id IS NEW.lease_id
      JOIN agent_control_worktree_reservation_states worktree
        ON worktree.reservation_id IS NEW.worktree_reservation_id
      WHERE stage.stage_run_id IS NEW.stage_run_id
        AND stage.project_id IS NEW.project_id AND stage.task_id IS NEW.task_id
        AND stage.attempt_id IS NEW.attempt_id AND stage.role_id IS 'verifier'
        AND stage.stage_kind IS 'verification' AND stage.stage_ordinal IS 3
        AND stage.attempt_ordinal IS 1 AND stage.status IS 'prepared'
        AND stage.task_revision IS NEW.task_revision
        AND stage.github_intake_sequence IS NEW.github_intake_sequence
        AND stage.source_identity_fingerprint IS NEW.source_identity_fingerprint
        AND lease.project_id IS NEW.project_id AND lease.task_id IS NEW.task_id
        AND lease.stage_run_id IS NEW.stage_run_id AND lease.attempt_id IS NEW.attempt_id
        AND lease.status IS 'reserved' AND lease.fence_token IS NEW.fence_token
        AND worktree.project_id IS NEW.project_id AND worktree.task_id IS NEW.task_id
        AND worktree.task_revision IS NEW.task_revision
        AND worktree.github_intake_sequence IS NEW.github_intake_sequence
        AND worktree.source_identity_fingerprint IS NEW.source_identity_fingerprint
        AND worktree.status IS 'ready' AND worktree.verified_at IS NOT NULL
        AND worktree.ownership_fingerprint IS NOT NULL
    )
    BEGIN SELECT RAISE(ABORT, 'verification reservation authority is invalid'); END
  `).unprepared;
  const projectionPredicate = `
    NEW.role_id = 'verifier' AND NEW.stage_kind = 'verification'
    AND NEW.stage_ordinal = 3 AND NEW.attempt_ordinal = 1
    AND json_valid(NEW.state_json) = 1 AND json_type(NEW.state_json) = 'object'
    AND json_extract(NEW.state_json, '$.schemaVersion') IS 1
    AND json_extract(NEW.state_json, '$.controlledThreadReservationId')
      IS NEW.controlled_thread_reservation_id
    AND json_extract(NEW.state_json, '$.threadId') IS NEW.thread_id
    AND json_extract(NEW.state_json, '$.projectId') IS NEW.project_id
    AND json_extract(NEW.state_json, '$.taskId') IS NEW.task_id
    AND json_extract(NEW.state_json, '$.taskRevision') IS NEW.task_revision
    AND json_extract(NEW.state_json, '$.githubIntakeSequence') IS NEW.github_intake_sequence
    AND json_extract(NEW.state_json, '$.sourceIdentityFingerprint')
      IS NEW.source_identity_fingerprint
    AND json_extract(NEW.state_json, '$.stageRunId') IS NEW.stage_run_id
    AND json_extract(NEW.state_json, '$.attemptId') IS NEW.attempt_id
    AND json_extract(NEW.state_json, '$.roleId') IS NEW.role_id
    AND json_extract(NEW.state_json, '$.stageKind') IS NEW.stage_kind
    AND json_extract(NEW.state_json, '$.stageOrdinal') IS NEW.stage_ordinal
    AND json_extract(NEW.state_json, '$.attemptOrdinal') IS NEW.attempt_ordinal
    AND json_extract(NEW.state_json, '$.leaseId') IS NEW.lease_id
    AND json_extract(NEW.state_json, '$.fenceToken') IS NEW.fence_token
    AND json_extract(NEW.state_json, '$.worktreeReservationId') IS NEW.worktree_reservation_id
    AND json_extract(NEW.state_json, '$.status') IS NEW.status
    AND json_extract(NEW.state_json, '$.revision') IS NEW.revision
    AND json_extract(NEW.state_json, '$.sequence') IS NEW.last_event_sequence
    AND json_extract(NEW.state_json, '$.preparedAt') IS NEW.prepared_at
    AND (
      (NEW.status = 'prepared' AND (SELECT count(*) FROM json_each(NEW.state_json)) = 21)
      OR (NEW.status = 'materializing'
        AND (SELECT count(*) FROM json_each(NEW.state_json)) = 28
        AND json_extract(NEW.state_json, '$.coordinatorCommandId') IS NEW.coordinator_command_id
        AND json_extract(NEW.state_json, '$.coordinatorCommandFingerprint')
          IS NEW.coordinator_command_fingerprint
        AND json_extract(NEW.state_json, '$.materializingTransitionCommandId')
          IS NEW.materializing_transition_command_id
        AND json_extract(NEW.state_json, '$.materializationCommandId')
          IS NEW.materialization_command_id
        AND json_extract(NEW.state_json, '$.materializationCommandFingerprint')
          IS NEW.materialization_command_fingerprint
        AND json_extract(NEW.state_json, '$.leaseHolderId') IS NEW.lease_holder_id
        AND json_extract(NEW.state_json, '$.materializingAt') IS NEW.materializing_at)
      OR (NEW.status = 'bound'
        AND (SELECT count(*) FROM json_each(NEW.state_json)) = 32
        AND json_extract(NEW.state_json, '$.coordinatorCommandId') IS NEW.coordinator_command_id
        AND json_extract(NEW.state_json, '$.coordinatorCommandFingerprint')
          IS NEW.coordinator_command_fingerprint
        AND json_extract(NEW.state_json, '$.materializingTransitionCommandId')
          IS NEW.materializing_transition_command_id
        AND json_extract(NEW.state_json, '$.materializationCommandId')
          IS NEW.materialization_command_id
        AND json_extract(NEW.state_json, '$.materializationCommandFingerprint')
          IS NEW.materialization_command_fingerprint
        AND json_extract(NEW.state_json, '$.leaseHolderId') IS NEW.lease_holder_id
        AND json_extract(NEW.state_json, '$.materializingAt') IS NEW.materializing_at
        AND json_extract(NEW.state_json, '$.boundTransitionCommandId')
          IS NEW.bound_transition_command_id
        AND json_extract(NEW.state_json, '$.orchestrationResultSequence')
          IS NEW.orchestration_result_sequence
        AND json_extract(NEW.state_json, '$.materializedAt') IS NEW.materialized_at
        AND json_extract(NEW.state_json, '$.boundAt') IS NEW.bound_at)
    )
    AND EXISTS (
      SELECT 1 FROM agent_control_verification_thread_stream_catalog catalog
      WHERE catalog.controlled_thread_reservation_id IS NEW.controlled_thread_reservation_id
        AND catalog.stream_version IS NEW.revision
        AND EXISTS (
          SELECT 1 FROM agent_control_events event
          WHERE event.event_id IS catalog.event_id
            AND event.sequence IS NEW.last_event_sequence
        )
    )
  `;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_projection_insert_validate
    BEFORE INSERT ON agent_control_verification_thread_reservation_states
    WHEN NOT COALESCE((${projectionPredicate}), 0)
    BEGIN SELECT RAISE(ABORT, 'verification reservation projection is invalid'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_projection_update_validate
    BEFORE UPDATE ON agent_control_verification_thread_reservation_states
    WHEN NOT COALESCE((
      (${projectionPredicate})
      AND NEW.controlled_thread_reservation_id IS OLD.controlled_thread_reservation_id
      AND NEW.thread_id IS OLD.thread_id AND NEW.project_id IS OLD.project_id
      AND NEW.task_id IS OLD.task_id AND NEW.task_revision IS OLD.task_revision
      AND NEW.github_intake_sequence IS OLD.github_intake_sequence
      AND NEW.source_identity_fingerprint IS OLD.source_identity_fingerprint
      AND NEW.stage_run_id IS OLD.stage_run_id AND NEW.attempt_id IS OLD.attempt_id
      AND NEW.role_id IS OLD.role_id AND NEW.stage_kind IS OLD.stage_kind
      AND NEW.stage_ordinal IS OLD.stage_ordinal
      AND NEW.attempt_ordinal IS OLD.attempt_ordinal
      AND NEW.lease_id IS OLD.lease_id AND NEW.fence_token IS OLD.fence_token
      AND NEW.worktree_reservation_id IS OLD.worktree_reservation_id
      AND NEW.prepared_at IS OLD.prepared_at
      AND NEW.revision IS OLD.revision + 1
      AND ((OLD.status = 'prepared' AND NEW.status = 'materializing')
        OR (OLD.status = 'materializing' AND NEW.status = 'bound'))
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification reservation transition'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_stream_catalog_no_delete
    BEFORE DELETE ON agent_control_verification_thread_stream_catalog
    BEGIN SELECT RAISE(ABORT, 'verification reservation evidence is immutable'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_thread_stream_catalog_no_update
    BEFORE UPDATE ON agent_control_verification_thread_stream_catalog
    BEGIN SELECT RAISE(ABORT, 'verification reservation evidence is immutable'); END
  `).unprepared;

  for (const trigger of referencedTriggers) {
    if (!trigger.name.startsWith("agent_control_verification_thread_")) {
      yield* sql.unsafe(trigger.sql).unprepared;
    }
  }
});

const expandVerificationStageBoundary = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DROP TRIGGER agent_control_verification_stage_event_validate`;
  yield* sql`DROP TRIGGER agent_control_verification_stage_projection_update_reject`;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_stage_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'stage-run'
      AND json_extract(NEW.payload_json, '$.stageKind') = 'verification'
      AND NOT COALESCE((
        (
          NEW.stream_version = 1
          AND NEW.event_type = 'agentControl.stageRun.prepared'
          AND NEW.actor_authority = 'controller'
          AND NEW.causation_event_id IS NULL
          AND json_extract(NEW.payload_json, '$.status') = 'prepared'
          AND json_extract(NEW.payload_json, '$.preparedAt') IS NEW.occurred_at
        ) OR (
          NEW.stream_version = 2
          AND NEW.event_type = 'agentControl.stageRun.verificationStarted'
          AND NEW.actor_authority = 'system'
          AND ${strictText("NEW.causation_event_id")}
          AND json_extract(NEW.payload_json, '$.status') = 'running'
          AND json_extract(NEW.payload_json, '$.startedAt') IS NEW.occurred_at
          AND json_extract(NEW.payload_json, '$.runtimeMode') = 'approval-required'
          AND (SELECT count(*) FROM agent_control_stage_run_states stage
            WHERE stage.stage_run_id IS NEW.stream_id
              AND stage.status = 'prepared' AND stage.revision = 1
              AND stage.role_id = 'verifier' AND stage.stage_kind = 'verification'
              AND stage.stage_ordinal = 3 AND stage.attempt_ordinal = 1) = 1
          AND (SELECT count(*) FROM agent_control_verification_deliveries delivery
            WHERE delivery.provider_delivery_id IS
              json_extract(NEW.payload_json, '$.providerDeliveryId')
              AND delivery.state = 'provider-started'
              AND delivery.provider_turn_id IS
                json_extract(NEW.payload_json, '$.providerTurnId')
              AND delivery.provider_instance_id IS
                json_extract(NEW.payload_json, '$.providerInstanceId')
              AND delivery.runtime_mode = 'approval-required'
              AND delivery.thread_id IS json_extract(NEW.payload_json, '$.threadId')
              AND delivery.stage_run_id IS NEW.stream_id
              AND delivery.lease_id IS json_extract(NEW.payload_json, '$.leaseId')
              AND delivery.lease_holder_id IS
                json_extract(NEW.payload_json, '$.leaseHolderId')
              AND delivery.fence_token IS json_extract(NEW.payload_json, '$.fenceToken')) = 1
        )
        AND ${strictText("NEW.event_id")}
        AND ${timestamp("NEW.occurred_at")}
        AND ${strictText("NEW.command_id")}
        AND NEW.correlation_id IS NEW.command_id
        AND ${canonicalJson("NEW.payload_json")}
        AND NEW.metadata_json = '{"schemaVersion":1}'
        AND NEW.stream_id IS json_extract(NEW.payload_json, '$.stageRunId')
        AND json_extract(NEW.payload_json, '$.roleId') = 'verifier'
        AND json_extract(NEW.payload_json, '$.stageOrdinal') = 3
        AND json_extract(NEW.payload_json, '$.attemptOrdinal') = 1
      ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification stage lifecycle event'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_stage_projection_update_validate
    BEFORE UPDATE ON agent_control_stage_run_states
    WHEN (OLD.stage_kind = 'verification' OR NEW.stage_kind = 'verification')
      AND NOT COALESCE((
      OLD.status = 'prepared' AND OLD.revision = 1
      AND NEW.status = 'running' AND NEW.revision = 2
      AND NEW.project_id IS OLD.project_id AND NEW.task_id IS OLD.task_id
      AND NEW.stage_run_id IS OLD.stage_run_id AND NEW.attempt_id IS OLD.attempt_id
      AND NEW.role_id = 'verifier' AND NEW.stage_kind = 'verification'
      AND NEW.stage_ordinal = 3 AND NEW.attempt_ordinal = 1
      AND NEW.task_revision IS OLD.task_revision
      AND NEW.github_intake_sequence IS OLD.github_intake_sequence
      AND NEW.source_identity_fingerprint IS OLD.source_identity_fingerprint
      AND NEW.created_at IS OLD.created_at AND ${timestamp("NEW.updated_at")}
      AND NEW.last_event_sequence > OLD.last_event_sequence
      AND ${canonicalJson("NEW.state_json")}
      AND NEW.state_json = json_object(
        'schemaVersion', 1, 'projectId', NEW.project_id, 'taskId', NEW.task_id,
        'stageRunId', NEW.stage_run_id, 'attemptId', NEW.attempt_id,
        'roleId', 'verifier', 'stageKind', 'verification',
        'stageOrdinal', 3, 'attemptOrdinal', 1, 'status', 'running',
        'taskRevision', NEW.task_revision,
        'githubIntakeSequence', NEW.github_intake_sequence,
        'sourceIdentityFingerprint', NEW.source_identity_fingerprint,
        'createdAt', NEW.created_at, 'updatedAt', NEW.updated_at,
        'revision', 2, 'sequence', NEW.last_event_sequence
      )
      AND (SELECT count(*) FROM agent_control_events event
        WHERE event.sequence IS NEW.last_event_sequence
          AND event.stream_id IS NEW.stage_run_id
          AND event.stream_version = 2
          AND event.event_type = 'agentControl.stageRun.verificationStarted') = 1
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification stage projection transition'); END
  `).unprepared;
});

const createVerificationEvidence = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE agent_control_verification_materialization_evidence (
      materialization_evidence_id TEXT PRIMARY KEY,
      materialization_command_id TEXT NOT NULL UNIQUE,
      materialization_fingerprint TEXT NOT NULL UNIQUE CHECK (
        length(materialization_fingerprint) = 64
        AND materialization_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      admission_evidence_id TEXT NOT NULL UNIQUE,
      admission_receipt_id TEXT NOT NULL UNIQUE,
      admission_marker_id TEXT NOT NULL UNIQUE,
      admission_fingerprint TEXT NOT NULL,
      admission_marker_fingerprint TEXT NOT NULL,
      implementation_result_evidence_id TEXT NOT NULL UNIQUE,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL,
      task_source_event_id TEXT NOT NULL UNIQUE,
      task_source_event_sequence INTEGER NOT NULL UNIQUE CHECK (
        task_source_event_sequence >= 1
      ),
      task_source_event_stream_version INTEGER NOT NULL CHECK (
        task_source_event_stream_version >= 1
      ),
      stage_run_id TEXT NOT NULL UNIQUE,
      attempt_id TEXT NOT NULL UNIQUE,
      lease_id TEXT NOT NULL,
      lease_holder_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
      worktree_reservation_id TEXT NOT NULL UNIQUE,
      worktree_revision INTEGER NOT NULL CHECK (worktree_revision >= 1),
      worktree_event_id TEXT NOT NULL UNIQUE,
      worktree_event_sequence INTEGER NOT NULL CHECK (worktree_event_sequence >= 1),
      worktree_event_stream_version INTEGER NOT NULL CHECK (
        worktree_event_stream_version >= 1
      ),
      worktree_ownership_fingerprint TEXT NOT NULL,
      worktree_verified_at TEXT NOT NULL,
      worktree_path TEXT NOT NULL,
      branch TEXT NOT NULL,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      coordinator_command_id TEXT NOT NULL UNIQUE,
      coordinator_command_fingerprint TEXT NOT NULL UNIQUE,
      reservation_materializing_event_id TEXT NOT NULL UNIQUE,
      reservation_materializing_event_sequence INTEGER NOT NULL UNIQUE,
      reservation_bound_event_id TEXT NOT NULL UNIQUE,
      reservation_bound_event_sequence INTEGER NOT NULL UNIQUE,
      orchestration_created_event_id TEXT NOT NULL UNIQUE,
      orchestration_created_event_sequence INTEGER NOT NULL UNIQUE,
      orchestration_bound_event_id TEXT NOT NULL UNIQUE,
      orchestration_bound_event_sequence INTEGER NOT NULL UNIQUE,
      orchestration_result_sequence INTEGER NOT NULL UNIQUE,
      planning_thread_id TEXT NOT NULL,
      plan_id TEXT NOT NULL,
      proposed_plan_json TEXT NOT NULL CHECK (json_valid(proposed_plan_json) = 1),
      proposed_plan_digest TEXT NOT NULL,
      model_selection_json TEXT NOT NULL CHECK (json_valid(model_selection_json) = 1),
      model_selection_fingerprint TEXT NOT NULL,
      implementation_handoff_json TEXT NOT NULL CHECK (json_valid(implementation_handoff_json) = 1),
      implementation_handoff_digest TEXT NOT NULL,
      implementation_provider_delivery_json TEXT NOT NULL CHECK (
        json_valid(implementation_provider_delivery_json) = 1
      ),
      implementation_provider_delivery_digest TEXT NOT NULL,
      implementation_result_json TEXT NOT NULL CHECK (json_valid(implementation_result_json) = 1),
      implementation_result_digest TEXT NOT NULL,
      verification_admission_json TEXT NOT NULL CHECK (json_valid(verification_admission_json) = 1),
      verification_admission_digest TEXT NOT NULL,
      verification_identity_json TEXT NOT NULL CHECK (json_valid(verification_identity_json) = 1),
      verification_identity_digest TEXT NOT NULL,
      task_history_json TEXT NOT NULL CHECK (json_valid(task_history_json) = 1),
      task_history_digest TEXT NOT NULL,
      worktree_history_json TEXT NOT NULL CHECK (json_valid(worktree_history_json) = 1),
      worktree_history_digest TEXT NOT NULL,
      stage_history_json TEXT NOT NULL CHECK (json_valid(stage_history_json) = 1),
      stage_history_digest TEXT NOT NULL,
      lease_history_json TEXT NOT NULL CHECK (json_valid(lease_history_json) = 1),
      lease_history_digest TEXT NOT NULL,
      reservation_history_json TEXT NOT NULL CHECK (json_valid(reservation_history_json) = 1),
      reservation_history_digest TEXT NOT NULL,
      orchestration_history_json TEXT NOT NULL CHECK (json_valid(orchestration_history_json) = 1),
      orchestration_history_digest TEXT NOT NULL,
      provider_instance_id TEXT NOT NULL,
      runtime_mode TEXT NOT NULL CHECK (runtime_mode = 'approval-required'),
      interaction_mode TEXT NOT NULL CHECK (interaction_mode = 'default'),
      repository_display TEXT NOT NULL,
      source_revision TEXT NOT NULL,
      task_title TEXT NOT NULL,
      task_body TEXT NOT NULL,
      materialized_at TEXT NOT NULL,
      FOREIGN KEY (admission_evidence_id)
        REFERENCES agent_control_verification_admission_evidence(admission_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (admission_receipt_id)
        REFERENCES agent_control_verification_admission_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (admission_marker_id)
        REFERENCES agent_control_verification_admission_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (implementation_result_evidence_id)
        REFERENCES agent_control_implementation_result_evidence(result_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (controlled_thread_reservation_id)
        REFERENCES agent_control_verification_thread_reservation_states(
          controlled_thread_reservation_id
        ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (materialization_command_id)
        REFERENCES orchestration_agent_control_thread_materialization_receipts(command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (task_source_event_id)
        REFERENCES agent_control_events(event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (
        worktree_event_id, worktree_reservation_id, worktree_event_stream_version
      ) REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_materialization_receipts (
      materialization_receipt_id TEXT PRIMARY KEY,
      materialization_evidence_id TEXT NOT NULL UNIQUE,
      materialization_command_id TEXT NOT NULL UNIQUE,
      materialization_fingerprint TEXT NOT NULL UNIQUE,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL CHECK (status = 'accepted'),
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (materialization_evidence_id)
        REFERENCES agent_control_verification_materialization_evidence(
          materialization_evidence_id
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_handoff_intents (
      handoff_id TEXT PRIMARY KEY,
      handoff_fingerprint TEXT NOT NULL UNIQUE,
      materialization_evidence_id TEXT NOT NULL UNIQUE,
      materialization_receipt_id TEXT NOT NULL UNIQUE,
      admission_marker_id TEXT NOT NULL UNIQUE,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL,
      task_source_event_id TEXT NOT NULL UNIQUE,
      task_source_event_sequence INTEGER NOT NULL UNIQUE CHECK (
        task_source_event_sequence >= 1
      ),
      task_source_event_stream_version INTEGER NOT NULL CHECK (
        task_source_event_stream_version >= 1
      ),
      stage_run_id TEXT NOT NULL UNIQUE,
      attempt_id TEXT NOT NULL UNIQUE,
      lease_id TEXT NOT NULL,
      lease_holder_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
      worktree_reservation_id TEXT NOT NULL UNIQUE,
      worktree_revision INTEGER NOT NULL CHECK (worktree_revision >= 1),
      worktree_event_id TEXT NOT NULL UNIQUE,
      worktree_event_sequence INTEGER NOT NULL CHECK (worktree_event_sequence >= 1),
      worktree_event_stream_version INTEGER NOT NULL CHECK (
        worktree_event_stream_version >= 1
      ),
      worktree_ownership_fingerprint TEXT NOT NULL,
      worktree_verified_at TEXT NOT NULL,
      worktree_path TEXT NOT NULL,
      branch TEXT NOT NULL,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      planning_thread_id TEXT NOT NULL,
      plan_id TEXT NOT NULL,
      proposed_plan_digest TEXT NOT NULL,
      provider_instance_id TEXT NOT NULL,
      runtime_mode TEXT NOT NULL CHECK (runtime_mode = 'approval-required'),
      model_selection_json TEXT NOT NULL CHECK (json_valid(model_selection_json) = 1),
      model_selection_fingerprint TEXT NOT NULL,
      template_version TEXT NOT NULL CHECK (
        template_version = 'agent-control-verification-prompt-v1'
      ),
      prompt_text TEXT NOT NULL CHECK (
        length(CAST(prompt_text AS BLOB)) BETWEEN 1 AND 1048576
      ),
      prompt_digest TEXT NOT NULL,
      turn_request_command_id TEXT NOT NULL UNIQUE,
      message_id TEXT NOT NULL UNIQUE,
      message_event_id TEXT NOT NULL UNIQUE,
      turn_request_event_id TEXT NOT NULL UNIQUE,
      message_event_template_json TEXT NOT NULL CHECK (json_valid(message_event_template_json) = 1),
      turn_request_event_template_json TEXT NOT NULL CHECK (
        json_valid(turn_request_event_template_json) = 1
      ),
      event_template_digest TEXT NOT NULL,
      provider_delivery_id TEXT NOT NULL UNIQUE,
      created_at TEXT NOT NULL,
      FOREIGN KEY (materialization_evidence_id)
        REFERENCES agent_control_verification_materialization_evidence(
          materialization_evidence_id
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (materialization_receipt_id)
        REFERENCES agent_control_verification_materialization_receipts(
          materialization_receipt_id
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_handoff_receipts (
      handoff_id TEXT PRIMARY KEY,
      handoff_fingerprint TEXT NOT NULL UNIQUE,
      materialization_evidence_id TEXT NOT NULL UNIQUE,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      turn_request_command_id TEXT NOT NULL UNIQUE,
      message_id TEXT NOT NULL UNIQUE,
      provider_delivery_id TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL CHECK (status = 'accepted'),
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_verification_handoff_intents(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_handoff_accepted (
      handoff_id TEXT PRIMARY KEY,
      handoff_fingerprint TEXT NOT NULL UNIQUE,
      materialization_evidence_id TEXT NOT NULL UNIQUE,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      turn_request_command_id TEXT NOT NULL UNIQUE,
      message_id TEXT NOT NULL UNIQUE,
      provider_delivery_id TEXT NOT NULL UNIQUE,
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_verification_handoff_receipts(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_deliveries (
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
        'provider-started', 'retry-wait', 'ambiguous'
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
      last_error_code TEXT,
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
          AND terminal_at IS NOT NULL AND last_error_code IS NOT NULL)
      ),
      CHECK (
        (state = 'provider-started'
          AND provider_turn_id IS NOT NULL AND provider_accepted_at IS NOT NULL)
        OR (state <> 'provider-started'
          AND provider_turn_id IS NULL AND provider_accepted_at IS NULL)
      ),
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_verification_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_verification_delivery_recovery
    ON agent_control_verification_deliveries(
      state, next_attempt_at, claim_expires_at, provider_accepted_at
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_turn_accepted (
      handoff_id TEXT PRIMARY KEY,
      handoff_fingerprint TEXT NOT NULL UNIQUE,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      planning_thread_id TEXT NOT NULL,
      plan_id TEXT NOT NULL,
      turn_request_command_id TEXT NOT NULL UNIQUE,
      message_id TEXT NOT NULL UNIQUE,
      message_event_id TEXT NOT NULL UNIQUE,
      message_event_sequence INTEGER NOT NULL UNIQUE,
      turn_request_event_id TEXT NOT NULL UNIQUE,
      turn_request_event_sequence INTEGER NOT NULL UNIQUE,
      message_event_envelope_json TEXT NOT NULL UNIQUE CHECK (
        json_valid(message_event_envelope_json) = 1
      ),
      turn_request_event_envelope_json TEXT NOT NULL UNIQUE CHECK (
        json_valid(turn_request_event_envelope_json) = 1
      ),
      event_evidence_digest TEXT NOT NULL UNIQUE,
      receipt_authority TEXT NOT NULL CHECK (receipt_authority = 'agent-control'),
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_verification_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (turn_request_command_id)
        REFERENCES orchestration_command_receipts(command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (message_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (turn_request_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_session_evidence (
      provider_delivery_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL UNIQUE,
      provider_instance_id TEXT NOT NULL,
      runtime_mode TEXT NOT NULL CHECK (runtime_mode = 'approval-required'),
      cwd TEXT NOT NULL,
      model_selection_json TEXT NOT NULL CHECK (json_valid(model_selection_json) = 1),
      model_selection_fingerprint TEXT NOT NULL,
      session_created_at TEXT NOT NULL,
      resume_cursor_json TEXT NOT NULL CHECK (json_valid(resume_cursor_json) = 1),
      recorded_at TEXT NOT NULL,
      FOREIGN KEY (provider_delivery_id)
        REFERENCES agent_control_verification_deliveries(provider_delivery_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_delivery_attestations (
      provider_delivery_id TEXT PRIMARY KEY,
      provider_instance_id TEXT NOT NULL,
      model_selection_json TEXT NOT NULL CHECK (json_valid(model_selection_json) = 1),
      model_selection_fingerprint TEXT NOT NULL,
      recorded_at TEXT NOT NULL,
      FOREIGN KEY (provider_delivery_id)
        REFERENCES agent_control_verification_deliveries(provider_delivery_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_stage_started_evidence (
      start_evidence_id TEXT PRIMARY KEY,
      start_command_id TEXT NOT NULL UNIQUE,
      start_fingerprint TEXT NOT NULL UNIQUE,
      admission_evidence_id TEXT NOT NULL UNIQUE,
      admission_receipt_id TEXT NOT NULL UNIQUE,
      admission_marker_id TEXT NOT NULL UNIQUE,
      materialization_evidence_id TEXT NOT NULL UNIQUE,
      materialization_receipt_id TEXT NOT NULL UNIQUE,
      materialization_marker_id TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL UNIQUE,
      handoff_fingerprint TEXT NOT NULL UNIQUE,
      provider_delivery_id TEXT NOT NULL UNIQUE,
      delivery_revision INTEGER NOT NULL CHECK (delivery_revision >= 1),
      claim_generation INTEGER NOT NULL CHECK (claim_generation >= 1),
      attempt_count INTEGER NOT NULL CHECK (attempt_count >= 1),
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL,
      stage_run_id TEXT NOT NULL UNIQUE,
      attempt_id TEXT NOT NULL UNIQUE,
      controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      thread_id TEXT NOT NULL UNIQUE,
      planning_thread_id TEXT NOT NULL,
      plan_id TEXT NOT NULL,
      proposed_plan_digest TEXT NOT NULL,
      lease_id TEXT NOT NULL,
      lease_holder_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
      provider_instance_id TEXT NOT NULL,
      provider_turn_id TEXT NOT NULL,
      runtime_mode TEXT NOT NULL CHECK (runtime_mode = 'approval-required'),
      model_selection_fingerprint TEXT NOT NULL,
      stage_event_id TEXT NOT NULL UNIQUE,
      stage_event_sequence INTEGER NOT NULL UNIQUE,
      stage_event_stream_version INTEGER NOT NULL CHECK (stage_event_stream_version = 2),
      started_at TEXT NOT NULL,
      FOREIGN KEY (provider_delivery_id)
        REFERENCES agent_control_verification_deliveries(provider_delivery_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (stage_event_id, stage_run_id, stage_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_stage_started_receipts (
      start_receipt_id TEXT PRIMARY KEY,
      start_evidence_id TEXT NOT NULL UNIQUE,
      start_command_id TEXT NOT NULL UNIQUE,
      start_fingerprint TEXT NOT NULL UNIQUE,
      provider_delivery_id TEXT NOT NULL UNIQUE,
      stage_event_id TEXT NOT NULL UNIQUE,
      stage_event_sequence INTEGER NOT NULL UNIQUE,
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (start_evidence_id)
        REFERENCES agent_control_verification_stage_started_evidence(start_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_stage_started_markers (
      start_marker_id TEXT PRIMARY KEY,
      start_evidence_id TEXT NOT NULL UNIQUE,
      start_receipt_id TEXT NOT NULL UNIQUE,
      start_command_id TEXT NOT NULL UNIQUE,
      start_fingerprint TEXT NOT NULL UNIQUE,
      provider_delivery_id TEXT NOT NULL UNIQUE,
      stage_event_id TEXT NOT NULL UNIQUE,
      stage_event_sequence INTEGER NOT NULL UNIQUE,
      committed_at TEXT NOT NULL,
      FOREIGN KEY (start_evidence_id)
        REFERENCES agent_control_verification_stage_started_evidence(start_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (start_receipt_id)
        REFERENCES agent_control_verification_stage_started_receipts(start_receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE agent_control_verification_materialization_markers (
      materialization_marker_id TEXT PRIMARY KEY,
      materialization_evidence_id TEXT NOT NULL UNIQUE,
      materialization_receipt_id TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL UNIQUE,
      provider_delivery_id TEXT NOT NULL UNIQUE,
      materialization_fingerprint TEXT NOT NULL UNIQUE,
      committed_at TEXT NOT NULL,
      FOREIGN KEY (materialization_evidence_id)
        REFERENCES agent_control_verification_materialization_evidence(
          materialization_evidence_id
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (materialization_receipt_id)
        REFERENCES agent_control_verification_materialization_receipts(
          materialization_receipt_id
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_verification_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  const storageTables: ReadonlyArray<
    readonly [table: string, columns: StorageColumns, validateUpdates?: boolean]
  > = [
    [
      "agent_control_verification_materialization_evidence",
      {
        text: [
          "materialization_evidence_id",
          "materialization_command_id",
          "admission_evidence_id",
          "admission_receipt_id",
          "admission_marker_id",
          "implementation_result_evidence_id",
          "project_id",
          "task_id",
          "task_source_event_id",
          "stage_run_id",
          "attempt_id",
          "lease_id",
          "lease_holder_id",
          "worktree_reservation_id",
          "worktree_event_id",
          "worktree_ownership_fingerprint",
          "worktree_path",
          "branch",
          "controlled_thread_reservation_id",
          "thread_id",
          "coordinator_command_id",
          "reservation_materializing_event_id",
          "reservation_bound_event_id",
          "orchestration_created_event_id",
          "orchestration_bound_event_id",
          "planning_thread_id",
          "plan_id",
          "provider_instance_id",
          "runtime_mode",
          "interaction_mode",
          "repository_display",
          "source_revision",
        ],
        textAllowEmpty: ["task_title", "task_body"],
        sha256: [
          "materialization_fingerprint",
          "admission_fingerprint",
          "admission_marker_fingerprint",
          "source_identity_fingerprint",
          "coordinator_command_fingerprint",
          "proposed_plan_digest",
          "model_selection_fingerprint",
          "implementation_handoff_digest",
          "implementation_provider_delivery_digest",
          "implementation_result_digest",
          "verification_admission_digest",
          "verification_identity_digest",
          "task_history_digest",
          "worktree_history_digest",
          "stage_history_digest",
          "lease_history_digest",
          "reservation_history_digest",
          "orchestration_history_digest",
        ],
        timestamp: ["worktree_verified_at", "materialized_at"],
        integer: [
          "task_revision",
          "github_intake_sequence",
          "task_source_event_sequence",
          "task_source_event_stream_version",
          "fence_token",
          "worktree_revision",
          "worktree_event_sequence",
          "worktree_event_stream_version",
          "reservation_materializing_event_sequence",
          "reservation_bound_event_sequence",
          "orchestration_created_event_sequence",
          "orchestration_bound_event_sequence",
          "orchestration_result_sequence",
        ],
        json: [
          "proposed_plan_json",
          "model_selection_json",
          "implementation_handoff_json",
          "implementation_provider_delivery_json",
          "implementation_result_json",
          "verification_admission_json",
          "verification_identity_json",
          "task_history_json",
          "worktree_history_json",
          "stage_history_json",
          "lease_history_json",
          "reservation_history_json",
          "orchestration_history_json",
        ],
      },
    ],
    [
      "agent_control_verification_materialization_receipts",
      {
        text: [
          "materialization_receipt_id",
          "materialization_evidence_id",
          "materialization_command_id",
          "controlled_thread_reservation_id",
          "thread_id",
          "status",
        ],
        sha256: ["materialization_fingerprint"],
        timestamp: ["accepted_at"],
      },
    ],
    [
      "agent_control_verification_handoff_intents",
      {
        text: [
          "handoff_id",
          "materialization_evidence_id",
          "materialization_receipt_id",
          "admission_marker_id",
          "project_id",
          "task_id",
          "task_source_event_id",
          "stage_run_id",
          "attempt_id",
          "lease_id",
          "lease_holder_id",
          "worktree_reservation_id",
          "worktree_event_id",
          "worktree_path",
          "branch",
          "controlled_thread_reservation_id",
          "thread_id",
          "planning_thread_id",
          "plan_id",
          "provider_instance_id",
          "runtime_mode",
          "template_version",
          "turn_request_command_id",
          "message_id",
          "message_event_id",
          "turn_request_event_id",
          "provider_delivery_id",
        ],
        uncheckedText: ["prompt_text"],
        sha256: [
          "handoff_fingerprint",
          "source_identity_fingerprint",
          "worktree_ownership_fingerprint",
          "proposed_plan_digest",
          "model_selection_fingerprint",
          "prompt_digest",
          "event_template_digest",
        ],
        timestamp: ["worktree_verified_at", "created_at"],
        integer: [
          "task_revision",
          "github_intake_sequence",
          "task_source_event_sequence",
          "task_source_event_stream_version",
          "fence_token",
          "worktree_revision",
          "worktree_event_sequence",
          "worktree_event_stream_version",
        ],
        json: [
          "model_selection_json",
          "message_event_template_json",
          "turn_request_event_template_json",
        ],
      },
    ],
    [
      "agent_control_verification_handoff_receipts",
      {
        text: [
          "handoff_id",
          "materialization_evidence_id",
          "controlled_thread_reservation_id",
          "thread_id",
          "turn_request_command_id",
          "message_id",
          "provider_delivery_id",
          "status",
        ],
        sha256: ["handoff_fingerprint"],
        timestamp: ["accepted_at"],
      },
    ],
    [
      "agent_control_verification_handoff_accepted",
      {
        text: [
          "handoff_id",
          "materialization_evidence_id",
          "controlled_thread_reservation_id",
          "thread_id",
          "turn_request_command_id",
          "message_id",
          "provider_delivery_id",
        ],
        sha256: ["handoff_fingerprint"],
        timestamp: ["accepted_at"],
      },
    ],
    [
      "agent_control_verification_deliveries",
      {
        text: [
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
        ],
        sha256: ["handoff_fingerprint", "model_selection_fingerprint"],
        timestamp: ["updated_at"],
        integer: [
          "fence_token",
          "revision",
          "claim_generation",
          "attempt_count",
          "interrupt_requested",
        ],
        nullableText: ["claim_owner_id", "provider_turn_id", "last_error_code"],
        nullableTimestamp: [
          "claim_expires_at",
          "next_attempt_at",
          "provider_accepted_at",
          "provider_session_created_at",
          "terminal_at",
        ],
        nullableJson: ["provider_resume_cursor_json"],
      },
      true,
    ],
    [
      "agent_control_verification_turn_accepted",
      {
        text: [
          "handoff_id",
          "controlled_thread_reservation_id",
          "thread_id",
          "planning_thread_id",
          "plan_id",
          "turn_request_command_id",
          "message_id",
          "message_event_id",
          "turn_request_event_id",
          "receipt_authority",
        ],
        sha256: ["handoff_fingerprint", "event_evidence_digest"],
        timestamp: ["accepted_at"],
        integer: ["message_event_sequence", "turn_request_event_sequence"],
        json: ["message_event_envelope_json", "turn_request_event_envelope_json"],
      },
    ],
    [
      "agent_control_verification_session_evidence",
      {
        text: ["provider_delivery_id", "thread_id", "provider_instance_id", "runtime_mode", "cwd"],
        sha256: ["model_selection_fingerprint"],
        timestamp: ["session_created_at", "recorded_at"],
        json: ["model_selection_json", "resume_cursor_json"],
      },
    ],
    [
      "agent_control_verification_delivery_attestations",
      {
        text: ["provider_delivery_id", "provider_instance_id"],
        sha256: ["model_selection_fingerprint"],
        timestamp: ["recorded_at"],
        json: ["model_selection_json"],
      },
    ],
    [
      "agent_control_verification_stage_started_evidence",
      {
        text: [
          "start_evidence_id",
          "start_command_id",
          "admission_evidence_id",
          "admission_receipt_id",
          "admission_marker_id",
          "materialization_evidence_id",
          "materialization_receipt_id",
          "materialization_marker_id",
          "handoff_id",
          "provider_delivery_id",
          "project_id",
          "task_id",
          "stage_run_id",
          "attempt_id",
          "controlled_thread_reservation_id",
          "thread_id",
          "planning_thread_id",
          "plan_id",
          "lease_id",
          "lease_holder_id",
          "provider_instance_id",
          "provider_turn_id",
          "runtime_mode",
          "stage_event_id",
        ],
        sha256: [
          "start_fingerprint",
          "handoff_fingerprint",
          "source_identity_fingerprint",
          "proposed_plan_digest",
          "model_selection_fingerprint",
        ],
        timestamp: ["started_at"],
        integer: [
          "delivery_revision",
          "claim_generation",
          "attempt_count",
          "task_revision",
          "github_intake_sequence",
          "fence_token",
          "stage_event_sequence",
          "stage_event_stream_version",
        ],
      },
    ],
    [
      "agent_control_verification_stage_started_receipts",
      {
        text: [
          "start_receipt_id",
          "start_evidence_id",
          "start_command_id",
          "provider_delivery_id",
          "stage_event_id",
        ],
        sha256: ["start_fingerprint"],
        timestamp: ["accepted_at"],
        integer: ["stage_event_sequence"],
      },
    ],
    [
      "agent_control_verification_stage_started_markers",
      {
        text: [
          "start_marker_id",
          "start_evidence_id",
          "start_receipt_id",
          "start_command_id",
          "provider_delivery_id",
          "stage_event_id",
        ],
        sha256: ["start_fingerprint"],
        timestamp: ["committed_at"],
        integer: ["stage_event_sequence"],
      },
    ],
    [
      "agent_control_verification_materialization_markers",
      {
        text: [
          "materialization_marker_id",
          "materialization_evidence_id",
          "materialization_receipt_id",
          "handoff_id",
          "provider_delivery_id",
        ],
        sha256: ["materialization_fingerprint"],
        timestamp: ["committed_at"],
      },
    ],
  ];
  for (const [table, columns, validateUpdates = false] of storageTables) {
    yield* sql.unsafe(`
      CREATE TRIGGER ${table}_storage_validate
      BEFORE INSERT ON ${table}
      WHEN NOT COALESCE((${storagePredicate(columns)}), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification evidence storage'); END
    `).unprepared;
    if (validateUpdates) {
      yield* sql.unsafe(`
        CREATE TRIGGER ${table}_update_storage_validate
        BEFORE UPDATE ON ${table}
        WHEN NOT COALESCE((${storagePredicate(columns)}), 0)
        BEGIN SELECT RAISE(ABORT, 'invalid verification evidence storage'); END
      `).unprepared;
    }
  }

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_materialization_evidence_validate
    BEFORE INSERT ON agent_control_verification_materialization_evidence
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_verification_admission_evidence admission
      JOIN agent_control_verification_admission_receipts admission_receipt
        ON admission_receipt.admission_evidence_id IS admission.admission_evidence_id
       AND admission_receipt.receipt_id IS NEW.admission_receipt_id
       AND admission_receipt.status IS 'accepted'
      JOIN agent_control_verification_admission_markers admission_marker
        ON admission_marker.admission_evidence_id IS admission.admission_evidence_id
       AND admission_marker.receipt_id IS admission_receipt.receipt_id
       AND admission_marker.marker_id IS NEW.admission_marker_id
      JOIN agent_control_implementation_result_evidence result
        ON result.result_evidence_id IS admission.implementation_result_evidence_id
       AND result.result_evidence_id IS NEW.implementation_result_evidence_id
      JOIN agent_control_verification_thread_reservation_states reservation
        ON reservation.controlled_thread_reservation_id IS
          admission.verification_controlled_thread_reservation_id
       AND reservation.controlled_thread_reservation_id IS
          NEW.controlled_thread_reservation_id
       AND reservation.status IS 'bound' AND reservation.revision IS 3
      JOIN agent_control_stage_run_states stage
        ON stage.stage_run_id IS admission.verification_stage_run_id
       AND stage.stage_run_id IS NEW.stage_run_id
       AND stage.status IS 'prepared' AND stage.revision IS 1
       AND stage.role_id IS 'verifier' AND stage.stage_kind IS 'verification'
       AND stage.stage_ordinal IS 3 AND stage.attempt_ordinal IS 1
      JOIN agent_control_stage_run_lease_states lease
        ON lease.lease_id IS admission.lease_id
       AND lease.lease_id IS NEW.lease_id
       AND lease.status IS 'reserved'
       AND lease.holder_id IS admission.lease_holder_id
       AND lease.holder_id IS NEW.lease_holder_id
       AND lease.fence_token IS admission.verification_fence_token
       AND lease.fence_token IS NEW.fence_token
      JOIN orchestration_agent_control_thread_materialization_intents intent
        ON intent.command_id IS NEW.materialization_command_id
       AND intent.receipt_status IS 'accepted'
       AND intent.accepted_receipt_command_id IS intent.command_id
      WHERE admission.admission_evidence_id IS NEW.admission_evidence_id
        AND admission.admission_fingerprint IS NEW.admission_fingerprint
        AND admission_marker.marker_fingerprint IS NEW.admission_marker_fingerprint
        AND admission.project_id IS NEW.project_id
        AND admission.task_id IS NEW.task_id
        AND admission.task_revision IS NEW.task_revision
        AND admission.github_intake_sequence IS NEW.github_intake_sequence
        AND admission.source_identity_fingerprint IS NEW.source_identity_fingerprint
        AND result.task_source_event_id IS NEW.task_source_event_id
        AND result.task_source_event_sequence IS NEW.task_source_event_sequence
        AND result.task_source_event_stream_version IS NEW.task_source_event_stream_version
        AND admission.verification_attempt_id IS NEW.attempt_id
        AND admission.worktree_reservation_id IS NEW.worktree_reservation_id
        AND admission.worktree_event_id IS NEW.worktree_event_id
        AND admission.worktree_event_sequence IS NEW.worktree_event_sequence
        AND admission.worktree_event_stream_version IS NEW.worktree_event_stream_version
        AND admission.worktree_ownership_fingerprint IS NEW.worktree_ownership_fingerprint
        AND result.worktree_revision IS NEW.worktree_revision
        AND admission.verification_thread_id IS NEW.thread_id
        AND admission.lease_id IS NEW.lease_id
        AND admission.lease_holder_id IS NEW.lease_holder_id
        AND admission.verification_fence_token IS NEW.fence_token
        AND admission.task_history_json IS NEW.task_history_json
        AND admission.task_history_digest IS NEW.task_history_digest
        AND admission.worktree_history_json IS NEW.worktree_history_json
        AND admission.worktree_history_digest IS NEW.worktree_history_digest
        AND admission.stage_history_json IS NEW.stage_history_json
        AND admission.stage_history_digest IS NEW.stage_history_digest
        AND admission.lease_history_json IS NEW.lease_history_json
        AND admission.lease_history_digest IS NEW.lease_history_digest
        AND admission.reservation_history_json IS NEW.reservation_history_json
        AND admission.reservation_history_digest IS NEW.reservation_history_digest
        AND admission.orchestration_history_json IS NEW.orchestration_history_json
        AND admission.orchestration_history_digest IS NEW.orchestration_history_digest
        AND reservation.thread_id IS NEW.thread_id
        AND reservation.stage_run_id IS NEW.stage_run_id
        AND reservation.attempt_id IS NEW.attempt_id
        AND reservation.lease_id IS NEW.lease_id
        AND reservation.fence_token IS NEW.fence_token
        AND reservation.worktree_reservation_id IS NEW.worktree_reservation_id
        AND reservation.coordinator_command_id IS NEW.coordinator_command_id
        AND reservation.coordinator_command_fingerprint IS
          NEW.coordinator_command_fingerprint
        AND reservation.materialization_command_id IS NEW.materialization_command_id
        AND reservation.materialization_command_fingerprint IS intent.command_fingerprint
        AND reservation.lease_holder_id IS NEW.lease_holder_id
        AND reservation.last_event_sequence IS NEW.reservation_bound_event_sequence
        AND reservation.orchestration_result_sequence IS NEW.orchestration_result_sequence
        AND intent.command_fingerprint IS reservation.materialization_command_fingerprint
        AND intent.controlled_thread_reservation_id IS NEW.controlled_thread_reservation_id
        AND intent.thread_id IS NEW.thread_id
        AND intent.project_id IS NEW.project_id
        AND intent.task_id IS NEW.task_id
        AND intent.task_revision IS NEW.task_revision
        AND intent.github_intake_sequence IS NEW.github_intake_sequence
        AND intent.source_identity_fingerprint IS NEW.source_identity_fingerprint
        AND intent.stage_run_id IS NEW.stage_run_id
        AND intent.attempt_id IS NEW.attempt_id
        AND intent.role_id IS 'verifier'
        AND intent.stage_kind IS 'verification'
        AND intent.stage_ordinal IS 3 AND intent.attempt_ordinal IS 1
        AND intent.lease_id IS NEW.lease_id
        AND intent.fence_token IS NEW.fence_token
        AND intent.worktree_reservation_id IS NEW.worktree_reservation_id
        AND intent.runtime_mode IS 'approval-required'
        AND intent.interaction_mode IS 'default'
        AND intent.source_proposed_plan_thread_id IS NEW.planning_thread_id
        AND intent.source_proposed_plan_id IS NEW.plan_id
        AND intent.created_event_id IS NEW.orchestration_created_event_id
        AND intent.created_event_sequence IS NEW.orchestration_created_event_sequence
        AND intent.binding_event_id IS NEW.orchestration_bound_event_id
        AND intent.binding_event_sequence IS NEW.orchestration_bound_event_sequence
        AND intent.receipt_result_sequence IS NEW.orchestration_result_sequence
    )
    BEGIN SELECT RAISE(ABORT, 'verification materialization evidence is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_handoff_intent_validate
    BEFORE INSERT ON agent_control_verification_handoff_intents
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_verification_materialization_evidence materialization
      JOIN agent_control_verification_materialization_receipts materialization_receipt
        ON materialization_receipt.materialization_evidence_id =
          materialization.materialization_evidence_id
      JOIN agent_control_verification_admission_evidence admission
        ON admission.admission_evidence_id = materialization.admission_evidence_id
      JOIN agent_control_verification_admission_receipts admission_receipt
        ON admission_receipt.admission_evidence_id = admission.admission_evidence_id
       AND admission_receipt.receipt_id = materialization.admission_receipt_id
      JOIN agent_control_verification_admission_markers admission_marker
        ON admission_marker.admission_evidence_id = admission.admission_evidence_id
       AND admission_marker.receipt_id = admission_receipt.receipt_id
       AND admission_marker.marker_id = materialization.admission_marker_id
      WHERE materialization.materialization_evidence_id IS NEW.materialization_evidence_id
        AND materialization_receipt.materialization_receipt_id IS
          NEW.materialization_receipt_id
        AND materialization_receipt.materialization_fingerprint IS
          materialization.materialization_fingerprint
        AND materialization_receipt.status IS 'accepted'
        AND NEW.admission_marker_id IS materialization.admission_marker_id
        AND admission.admission_fingerprint IS materialization.admission_fingerprint
        AND admission_marker.marker_fingerprint IS
          materialization.admission_marker_fingerprint
        AND NEW.project_id IS materialization.project_id
        AND NEW.task_id IS materialization.task_id
        AND NEW.task_revision IS materialization.task_revision
        AND NEW.github_intake_sequence IS materialization.github_intake_sequence
        AND NEW.source_identity_fingerprint IS materialization.source_identity_fingerprint
        AND NEW.task_source_event_id IS materialization.task_source_event_id
        AND NEW.task_source_event_sequence IS materialization.task_source_event_sequence
        AND NEW.task_source_event_stream_version IS
          materialization.task_source_event_stream_version
        AND NEW.stage_run_id IS materialization.stage_run_id
        AND NEW.attempt_id IS materialization.attempt_id
        AND NEW.lease_id IS materialization.lease_id
        AND NEW.lease_holder_id IS materialization.lease_holder_id
        AND NEW.fence_token IS materialization.fence_token
        AND NEW.worktree_reservation_id IS materialization.worktree_reservation_id
        AND NEW.worktree_revision IS materialization.worktree_revision
        AND NEW.worktree_event_id IS materialization.worktree_event_id
        AND NEW.worktree_event_sequence IS materialization.worktree_event_sequence
        AND NEW.worktree_event_stream_version IS
          materialization.worktree_event_stream_version
        AND NEW.worktree_ownership_fingerprint IS
          materialization.worktree_ownership_fingerprint
        AND NEW.worktree_verified_at IS materialization.worktree_verified_at
        AND NEW.worktree_path IS materialization.worktree_path
        AND NEW.branch IS materialization.branch
        AND NEW.controlled_thread_reservation_id IS
          materialization.controlled_thread_reservation_id
        AND NEW.thread_id IS materialization.thread_id
        AND NEW.planning_thread_id IS materialization.planning_thread_id
        AND NEW.plan_id IS materialization.plan_id
        AND NEW.proposed_plan_digest IS materialization.proposed_plan_digest
        AND NEW.provider_instance_id IS materialization.provider_instance_id
        AND NEW.runtime_mode IS materialization.runtime_mode
        AND NEW.model_selection_json IS materialization.model_selection_json
        AND NEW.model_selection_fingerprint IS materialization.model_selection_fingerprint
        AND NEW.template_version IS 'agent-control-verification-prompt-v1'
        AND NEW.created_at IS materialization.materialized_at
        AND length(CAST(NEW.prompt_text AS BLOB)) <= 1048576
        AND json_extract(NEW.message_event_template_json, '$.aggregateId') IS NEW.thread_id
        AND json_extract(NEW.message_event_template_json, '$.eventId') IS
          NEW.message_event_id
        AND json_extract(NEW.message_event_template_json, '$.commandId') IS
          NEW.turn_request_command_id
        AND json_extract(NEW.message_event_template_json, '$.correlationId') IS
          NEW.turn_request_command_id
        AND json_extract(NEW.message_event_template_json, '$.occurredAt') IS NEW.created_at
        AND json_extract(NEW.message_event_template_json, '$.streamVersion') IS 3
        AND json_extract(NEW.message_event_template_json, '$.type') IS 'thread.message-sent'
        AND json_extract(NEW.message_event_template_json, '$.actorKind') IS 'client'
        AND json_extract(NEW.message_event_template_json, '$.payload.threadId') IS NEW.thread_id
        AND json_extract(NEW.message_event_template_json, '$.payload.messageId') IS NEW.message_id
        AND json_extract(NEW.message_event_template_json, '$.payload.text') IS NEW.prompt_text
        AND json_extract(NEW.message_event_template_json, '$.payload.createdAt') IS NEW.created_at
        AND json_extract(NEW.turn_request_event_template_json, '$.aggregateId') IS NEW.thread_id
        AND json_extract(NEW.turn_request_event_template_json, '$.eventId') IS
          NEW.turn_request_event_id
        AND json_extract(NEW.turn_request_event_template_json, '$.commandId') IS
          NEW.turn_request_command_id
        AND json_extract(NEW.turn_request_event_template_json, '$.correlationId') IS
          NEW.turn_request_command_id
        AND json_extract(NEW.turn_request_event_template_json, '$.causationEventId') IS
          NEW.message_event_id
        AND json_extract(NEW.turn_request_event_template_json, '$.occurredAt') IS NEW.created_at
        AND json_extract(NEW.turn_request_event_template_json, '$.streamVersion') IS 4
        AND json_extract(NEW.turn_request_event_template_json, '$.type') IS
          'thread.turn-start-requested'
        AND json_extract(NEW.turn_request_event_template_json, '$.actorKind') IS 'client'
        AND json_extract(NEW.turn_request_event_template_json, '$.payload.threadId') IS
          NEW.thread_id
        AND json_extract(NEW.turn_request_event_template_json, '$.payload.messageId') IS
          NEW.message_id
        AND json_extract(NEW.turn_request_event_template_json, '$.payload.runtimeMode') IS
          NEW.runtime_mode
        AND json_extract(NEW.turn_request_event_template_json, '$.payload.interactionMode') IS
          'default'
        AND json_extract(
          NEW.turn_request_event_template_json,
          '$.payload.sourceProposedPlan.threadId'
        ) IS materialization.planning_thread_id
        AND json_extract(
          NEW.turn_request_event_template_json,
          '$.payload.sourceProposedPlan.planId'
        ) IS materialization.plan_id
        AND json_extract(NEW.turn_request_event_template_json, '$.payload.modelSelection') IS
          json(NEW.model_selection_json)
    )
    BEGIN SELECT RAISE(ABORT, 'verification handoff intent is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_materialization_receipt_validate
    BEFORE INSERT ON agent_control_verification_materialization_receipts
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_verification_materialization_evidence evidence
      WHERE evidence.materialization_evidence_id = NEW.materialization_evidence_id
        AND evidence.materialization_command_id = NEW.materialization_command_id
        AND evidence.materialization_fingerprint = NEW.materialization_fingerprint
        AND evidence.controlled_thread_reservation_id =
          NEW.controlled_thread_reservation_id
        AND evidence.thread_id = NEW.thread_id
        AND evidence.materialized_at = NEW.accepted_at
    )
    BEGIN SELECT RAISE(ABORT, 'verification materialization receipt is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_handoff_receipt_validate
    BEFORE INSERT ON agent_control_verification_handoff_receipts
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_verification_handoff_intents intent
      WHERE intent.handoff_id = NEW.handoff_id
        AND intent.handoff_fingerprint = NEW.handoff_fingerprint
        AND intent.materialization_evidence_id = NEW.materialization_evidence_id
        AND intent.controlled_thread_reservation_id =
          NEW.controlled_thread_reservation_id
        AND intent.thread_id = NEW.thread_id
        AND intent.turn_request_command_id = NEW.turn_request_command_id
        AND intent.message_id = NEW.message_id
        AND intent.provider_delivery_id = NEW.provider_delivery_id
        AND intent.created_at = NEW.accepted_at
    )
    BEGIN SELECT RAISE(ABORT, 'verification handoff receipt is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_handoff_accepted_validate
    BEFORE INSERT ON agent_control_verification_handoff_accepted
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_verification_handoff_receipts receipt
      WHERE receipt.handoff_id = NEW.handoff_id
        AND receipt.handoff_fingerprint = NEW.handoff_fingerprint
        AND receipt.materialization_evidence_id = NEW.materialization_evidence_id
        AND receipt.controlled_thread_reservation_id =
          NEW.controlled_thread_reservation_id
        AND receipt.thread_id = NEW.thread_id
        AND receipt.turn_request_command_id = NEW.turn_request_command_id
        AND receipt.message_id = NEW.message_id
        AND receipt.provider_delivery_id = NEW.provider_delivery_id
        AND receipt.accepted_at = NEW.accepted_at
    )
    BEGIN SELECT RAISE(ABORT, 'verification handoff acceptance is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_delivery_insert_validate
    BEFORE INSERT ON agent_control_verification_deliveries
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_verification_handoff_intents intent
      JOIN agent_control_verification_handoff_accepted accepted
        ON accepted.handoff_id = intent.handoff_id
      WHERE accepted.provider_delivery_id = NEW.provider_delivery_id
        AND intent.handoff_id = NEW.handoff_id
        AND intent.handoff_fingerprint = NEW.handoff_fingerprint
        AND intent.admission_marker_id = NEW.admission_marker_id
        AND intent.materialization_evidence_id = NEW.materialization_evidence_id
        AND intent.controlled_thread_reservation_id =
          NEW.controlled_thread_reservation_id
        AND intent.thread_id = NEW.thread_id
        AND intent.stage_run_id = NEW.stage_run_id AND intent.attempt_id = NEW.attempt_id
        AND intent.lease_id = NEW.lease_id AND intent.lease_holder_id = NEW.lease_holder_id
        AND intent.fence_token = NEW.fence_token
        AND intent.provider_instance_id = NEW.provider_instance_id
        AND intent.runtime_mode = NEW.runtime_mode
        AND intent.model_selection_fingerprint = NEW.model_selection_fingerprint
        AND intent.turn_request_command_id = NEW.turn_request_command_id
        AND intent.message_id = NEW.message_id
        AND intent.planning_thread_id = NEW.planning_thread_id
        AND intent.plan_id = NEW.plan_id
        AND NEW.state = 'pending' AND NEW.revision = 0
        AND NEW.claim_generation = 0 AND NEW.attempt_count = 0
        AND NEW.updated_at = intent.created_at
    )
    BEGIN SELECT RAISE(ABORT, 'verification delivery identity is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_materialization_marker_validate
    BEFORE INSERT ON agent_control_verification_materialization_markers
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_verification_materialization_evidence evidence
      JOIN agent_control_verification_materialization_receipts receipt
        ON receipt.materialization_evidence_id = evidence.materialization_evidence_id
      JOIN agent_control_verification_handoff_accepted accepted
        ON accepted.materialization_evidence_id = evidence.materialization_evidence_id
      JOIN agent_control_verification_deliveries delivery
        ON delivery.handoff_id = accepted.handoff_id
      WHERE evidence.materialization_evidence_id = NEW.materialization_evidence_id
        AND receipt.materialization_receipt_id = NEW.materialization_receipt_id
        AND accepted.handoff_id = NEW.handoff_id
        AND delivery.provider_delivery_id = NEW.provider_delivery_id
        AND evidence.materialization_fingerprint = NEW.materialization_fingerprint
        AND receipt.materialization_fingerprint = NEW.materialization_fingerprint
        AND evidence.materialized_at = NEW.committed_at
        AND receipt.accepted_at = NEW.committed_at
        AND accepted.accepted_at = NEW.committed_at
    )
    BEGIN SELECT RAISE(ABORT, 'verification materialization marker is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_turn_accepted_validate
    BEFORE INSERT ON agent_control_verification_turn_accepted
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_verification_handoff_intents intent
      JOIN agent_control_verification_handoff_accepted accepted
        ON accepted.handoff_id = intent.handoff_id
      JOIN orchestration_command_receipts receipt
        ON receipt.command_id = NEW.turn_request_command_id
      JOIN orchestration_events message_event ON message_event.event_id = NEW.message_event_id
      JOIN orchestration_events turn_event ON turn_event.event_id = NEW.turn_request_event_id
      WHERE intent.handoff_id = NEW.handoff_id
        AND intent.handoff_fingerprint = NEW.handoff_fingerprint
        AND intent.controlled_thread_reservation_id =
          NEW.controlled_thread_reservation_id
        AND intent.thread_id = NEW.thread_id
        AND intent.planning_thread_id = NEW.planning_thread_id
        AND intent.plan_id = NEW.plan_id
        AND intent.turn_request_command_id = NEW.turn_request_command_id
        AND intent.message_id = NEW.message_id
        AND intent.message_event_id = NEW.message_event_id
        AND intent.turn_request_event_id = NEW.turn_request_event_id
        AND receipt.status = 'accepted' AND receipt.authority = 'agent-control'
        AND receipt.result_sequence = NEW.turn_request_event_sequence
        AND message_event.sequence = NEW.message_event_sequence
        AND message_event.event_type = 'thread.message-sent'
        AND message_event.command_id = NEW.turn_request_command_id
        AND json_extract(message_event.payload_json, '$.threadId') = NEW.thread_id
        AND json_extract(message_event.payload_json, '$.messageId') = NEW.message_id
        AND json_extract(message_event.payload_json, '$.role') = 'user'
        AND json_extract(message_event.payload_json, '$.text') = intent.prompt_text
        AND json_array_length(message_event.payload_json, '$.attachments') = 0
        AND turn_event.sequence = NEW.turn_request_event_sequence
        AND turn_event.event_type = 'thread.turn-start-requested'
        AND turn_event.command_id = NEW.turn_request_command_id
        AND json_extract(turn_event.payload_json, '$.threadId') = NEW.thread_id
        AND json_extract(turn_event.payload_json, '$.messageId') = NEW.message_id
        AND json_extract(turn_event.payload_json, '$.interactionMode') = 'default'
        AND json_extract(turn_event.payload_json, '$.runtimeMode') = intent.runtime_mode
        AND json_extract(turn_event.payload_json, '$.sourceProposedPlan.threadId') =
          NEW.planning_thread_id
        AND json_extract(turn_event.payload_json, '$.sourceProposedPlan.planId') = NEW.plan_id
        AND json_extract(NEW.message_event_envelope_json, '$.eventId') = NEW.message_event_id
        AND json_extract(NEW.message_event_envelope_json, '$.sequence') =
          NEW.message_event_sequence
        AND json_extract(NEW.turn_request_event_envelope_json, '$.eventId') =
          NEW.turn_request_event_id
        AND json_extract(NEW.turn_request_event_envelope_json, '$.sequence') =
          NEW.turn_request_event_sequence
        AND json_extract(NEW.turn_request_event_envelope_json,
          '$.payload.sourceProposedPlan.threadId') = NEW.planning_thread_id
        AND json_extract(NEW.turn_request_event_envelope_json,
          '$.payload.sourceProposedPlan.planId') = NEW.plan_id
        AND NEW.turn_request_event_sequence = NEW.message_event_sequence + 1
        AND NEW.receipt_authority = 'agent-control'
        AND NEW.accepted_at = intent.created_at
    )
    BEGIN SELECT RAISE(ABORT, 'verification turn acceptance is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_session_evidence_validate
    BEFORE INSERT ON agent_control_verification_session_evidence
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_verification_deliveries delivery
      JOIN agent_control_verification_handoff_intents intent
        ON intent.handoff_id = delivery.handoff_id
      WHERE delivery.provider_delivery_id = NEW.provider_delivery_id
        AND delivery.state = 'claimed'
        AND delivery.thread_id = NEW.thread_id
        AND delivery.provider_instance_id = NEW.provider_instance_id
        AND delivery.runtime_mode = NEW.runtime_mode
        AND intent.worktree_path = NEW.cwd
        AND delivery.model_selection_fingerprint = NEW.model_selection_fingerprint
    )
    BEGIN SELECT RAISE(ABORT, 'verification session evidence is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_delivery_attestation_validate
    BEFORE INSERT ON agent_control_verification_delivery_attestations
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_verification_deliveries delivery
      JOIN agent_control_verification_session_evidence session
        ON session.provider_delivery_id = delivery.provider_delivery_id
      WHERE delivery.provider_delivery_id = NEW.provider_delivery_id
        AND delivery.state = 'claimed'
        AND delivery.provider_instance_id = NEW.provider_instance_id
        AND delivery.model_selection_fingerprint = NEW.model_selection_fingerprint
        AND session.model_selection_json = NEW.model_selection_json
        AND session.model_selection_fingerprint = NEW.model_selection_fingerprint
    )
    BEGIN SELECT RAISE(ABORT, 'verification delivery attestation is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_stage_started_evidence_validate
    BEFORE INSERT ON agent_control_verification_stage_started_evidence
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_verification_deliveries delivery
      JOIN agent_control_verification_handoff_intents intent
        ON intent.handoff_id = delivery.handoff_id
      JOIN agent_control_events event ON event.event_id = NEW.stage_event_id
      JOIN agent_control_stage_run_states stage ON stage.stage_run_id = NEW.stage_run_id
      JOIN agent_control_stage_run_lease_states lease ON lease.lease_id = NEW.lease_id
      WHERE delivery.provider_delivery_id = NEW.provider_delivery_id
        AND delivery.revision = NEW.delivery_revision
        AND delivery.claim_generation = NEW.claim_generation
        AND delivery.attempt_count = NEW.attempt_count
        AND delivery.provider_turn_id = NEW.provider_turn_id
        AND delivery.provider_accepted_at = NEW.started_at
        AND intent.project_id = NEW.project_id AND intent.task_id = NEW.task_id
        AND intent.task_revision = NEW.task_revision
        AND intent.github_intake_sequence = NEW.github_intake_sequence
        AND intent.source_identity_fingerprint = NEW.source_identity_fingerprint
        AND intent.stage_run_id = NEW.stage_run_id AND intent.attempt_id = NEW.attempt_id
        AND intent.controlled_thread_reservation_id =
          NEW.controlled_thread_reservation_id
        AND intent.thread_id = NEW.thread_id
        AND intent.planning_thread_id = NEW.planning_thread_id
        AND intent.plan_id = NEW.plan_id
        AND intent.proposed_plan_digest = NEW.proposed_plan_digest
        AND intent.lease_id = NEW.lease_id AND intent.lease_holder_id = NEW.lease_holder_id
        AND intent.fence_token = NEW.fence_token
        AND intent.provider_instance_id = NEW.provider_instance_id
        AND intent.runtime_mode = NEW.runtime_mode
        AND intent.model_selection_fingerprint = NEW.model_selection_fingerprint
        AND event.aggregate_kind = 'stage-run' AND event.stream_id = NEW.stage_run_id
        AND event.stream_version = 2
        AND event.event_type = 'agentControl.stageRun.verificationStarted'
        AND event.command_id = NEW.start_command_id
        AND event.causation_event_id = intent.turn_request_event_id
        AND event.sequence = NEW.stage_event_sequence
        AND event.actor_authority = 'system'
        AND json_extract(event.payload_json, '$.projectId') = NEW.project_id
        AND json_extract(event.payload_json, '$.taskId') = NEW.task_id
        AND json_extract(event.payload_json, '$.taskRevision') = NEW.task_revision
        AND json_extract(event.payload_json, '$.githubIntakeSequence') =
          NEW.github_intake_sequence
        AND json_extract(event.payload_json, '$.sourceIdentityFingerprint') =
          NEW.source_identity_fingerprint
        AND json_extract(event.payload_json, '$.stageRunId') = NEW.stage_run_id
        AND json_extract(event.payload_json, '$.attemptId') = NEW.attempt_id
        AND json_extract(event.payload_json, '$.roleId') = 'verifier'
        AND json_extract(event.payload_json, '$.stageKind') = 'verification'
        AND json_extract(event.payload_json, '$.stageOrdinal') = 3
        AND json_extract(event.payload_json, '$.attemptOrdinal') = 1
        AND json_extract(event.payload_json, '$.status') = 'running'
        AND json_extract(event.payload_json, '$.admissionEvidenceId') =
          NEW.admission_evidence_id
        AND json_extract(event.payload_json, '$.admissionReceiptId') =
          NEW.admission_receipt_id
        AND json_extract(event.payload_json, '$.admissionMarkerId') =
          NEW.admission_marker_id
        AND json_extract(event.payload_json, '$.materializationEvidenceId') =
          NEW.materialization_evidence_id
        AND json_extract(event.payload_json, '$.materializationReceiptId') =
          NEW.materialization_receipt_id
        AND json_extract(event.payload_json, '$.materializationMarkerId') =
          NEW.materialization_marker_id
        AND json_extract(event.payload_json, '$.providerDeliveryId') =
          NEW.provider_delivery_id
        AND json_extract(event.payload_json, '$.deliveryRevision') = NEW.delivery_revision
        AND json_extract(event.payload_json, '$.claimGeneration') = NEW.claim_generation
        AND json_extract(event.payload_json, '$.attemptCount') = NEW.attempt_count
        AND json_extract(event.payload_json, '$.providerTurnId') = NEW.provider_turn_id
        AND json_extract(event.payload_json, '$.handoffId') = NEW.handoff_id
        AND json_extract(event.payload_json, '$.handoffFingerprint') =
          NEW.handoff_fingerprint
        AND json_extract(event.payload_json, '$.controlledThreadReservationId') =
          NEW.controlled_thread_reservation_id
        AND json_extract(event.payload_json, '$.threadId') = NEW.thread_id
        AND json_extract(event.payload_json, '$.planningThreadId') = NEW.planning_thread_id
        AND json_extract(event.payload_json, '$.planId') = NEW.plan_id
        AND json_extract(event.payload_json, '$.proposedPlanDigest') =
          NEW.proposed_plan_digest
        AND json_extract(event.payload_json, '$.providerInstanceId') =
          NEW.provider_instance_id
        AND json_extract(event.payload_json, '$.runtimeMode') = NEW.runtime_mode
        AND json_extract(event.payload_json, '$.modelSelectionFingerprint') =
          NEW.model_selection_fingerprint
        AND json_extract(event.payload_json, '$.leaseId') = NEW.lease_id
        AND json_extract(event.payload_json, '$.leaseHolderId') = NEW.lease_holder_id
        AND json_extract(event.payload_json, '$.fenceToken') = NEW.fence_token
        AND json_extract(event.payload_json, '$.startedAt') = NEW.started_at
        AND stage.status = 'running' AND stage.revision = 2
        AND lease.status = 'reserved' AND lease.stage_run_id = NEW.stage_run_id
        AND lease.attempt_id = NEW.attempt_id AND lease.holder_id = NEW.lease_holder_id
        AND lease.fence_token = NEW.fence_token
    )
    BEGIN SELECT RAISE(ABORT, 'verification stage-start evidence is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_stage_started_receipt_validate
    BEFORE INSERT ON agent_control_verification_stage_started_receipts
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_verification_stage_started_evidence evidence
      WHERE evidence.start_evidence_id = NEW.start_evidence_id
        AND evidence.start_command_id = NEW.start_command_id
        AND evidence.start_fingerprint = NEW.start_fingerprint
        AND evidence.provider_delivery_id = NEW.provider_delivery_id
        AND evidence.stage_event_id = NEW.stage_event_id
        AND evidence.stage_event_sequence = NEW.stage_event_sequence
        AND evidence.started_at = NEW.accepted_at
    )
    BEGIN SELECT RAISE(ABORT, 'verification stage-start receipt is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_stage_started_marker_validate
    BEFORE INSERT ON agent_control_verification_stage_started_markers
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_verification_stage_started_evidence evidence
      JOIN agent_control_verification_stage_started_receipts receipt
        ON receipt.start_evidence_id = evidence.start_evidence_id
      WHERE evidence.start_evidence_id = NEW.start_evidence_id
        AND receipt.start_receipt_id = NEW.start_receipt_id
        AND evidence.start_command_id = NEW.start_command_id
        AND evidence.start_fingerprint = NEW.start_fingerprint
        AND evidence.provider_delivery_id = NEW.provider_delivery_id
        AND evidence.stage_event_id = NEW.stage_event_id
        AND evidence.stage_event_sequence = NEW.stage_event_sequence
        AND evidence.started_at = NEW.committed_at
        AND receipt.accepted_at = NEW.committed_at
    )
    BEGIN SELECT RAISE(ABORT, 'verification stage-start marker is inconsistent'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_task_source_event_no_update
    BEFORE UPDATE ON agent_control_events
    WHEN EXISTS (
      SELECT 1 FROM agent_control_verification_materialization_evidence evidence
      WHERE evidence.task_source_event_id IS OLD.event_id
    )
    BEGIN SELECT RAISE(ABORT, 'verification task source evidence is immutable'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_verification_task_source_event_no_delete
    BEFORE DELETE ON agent_control_events
    WHEN EXISTS (
      SELECT 1 FROM agent_control_verification_materialization_evidence evidence
      WHERE evidence.task_source_event_id IS OLD.event_id
    )
    BEGIN SELECT RAISE(ABORT, 'verification task source evidence is immutable'); END
  `).unprepared;

  const immutableTables = [
    "agent_control_verification_materialization_evidence",
    "agent_control_verification_materialization_receipts",
    "agent_control_verification_materialization_markers",
    "agent_control_verification_handoff_intents",
    "agent_control_verification_handoff_receipts",
    "agent_control_verification_handoff_accepted",
    "agent_control_verification_turn_accepted",
    "agent_control_verification_session_evidence",
    "agent_control_verification_delivery_attestations",
    "agent_control_verification_stage_started_evidence",
    "agent_control_verification_stage_started_receipts",
    "agent_control_verification_stage_started_markers",
  ] as const;
  for (const table of immutableTables) {
    yield* sql.unsafe(`
      CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
      BEGIN SELECT RAISE(ABORT, 'verification evidence is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table}
      BEGIN SELECT RAISE(ABORT, 'verification evidence is immutable'); END
    `).unprepared;
  }
  yield* sql`
    CREATE TRIGGER agent_control_verification_deliveries_no_delete
    BEFORE DELETE ON agent_control_verification_deliveries
    BEGIN SELECT RAISE(ABORT, 'verification delivery is immutable'); END
  `;
  yield* sql`
    CREATE TRIGGER agent_control_verification_delivery_transition_validate
    BEFORE UPDATE ON agent_control_verification_deliveries
    WHEN
      NEW.provider_delivery_id IS NOT OLD.provider_delivery_id
      OR NEW.handoff_id IS NOT OLD.handoff_id
      OR NEW.handoff_fingerprint IS NOT OLD.handoff_fingerprint
      OR NEW.admission_marker_id IS NOT OLD.admission_marker_id
      OR NEW.materialization_evidence_id IS NOT OLD.materialization_evidence_id
      OR NEW.controlled_thread_reservation_id IS NOT OLD.controlled_thread_reservation_id
      OR NEW.thread_id IS NOT OLD.thread_id OR NEW.stage_run_id IS NOT OLD.stage_run_id
      OR NEW.attempt_id IS NOT OLD.attempt_id OR NEW.lease_id IS NOT OLD.lease_id
      OR NEW.lease_holder_id IS NOT OLD.lease_holder_id
      OR NEW.fence_token IS NOT OLD.fence_token
      OR NEW.provider_instance_id IS NOT OLD.provider_instance_id
      OR NEW.runtime_mode IS NOT OLD.runtime_mode
      OR NEW.model_selection_fingerprint IS NOT OLD.model_selection_fingerprint
      OR NEW.turn_request_command_id IS NOT OLD.turn_request_command_id
      OR NEW.message_id IS NOT OLD.message_id
      OR NEW.planning_thread_id IS NOT OLD.planning_thread_id
      OR NEW.plan_id IS NOT OLD.plan_id
      OR NEW.revision IS NOT OLD.revision + 1
      OR NOT (
        (OLD.state = 'pending' AND NEW.state = 'turn-accepted'
          AND EXISTS (
            SELECT 1 FROM agent_control_verification_turn_accepted accepted
            WHERE accepted.handoff_id = NEW.handoff_id
              AND accepted.turn_request_command_id = NEW.turn_request_command_id
          ))
        OR (OLD.state IN ('turn-accepted', 'retry-wait', 'claimed') AND NEW.state = 'claimed'
          AND NEW.claim_generation = OLD.claim_generation + 1
          AND NEW.attempt_count = OLD.attempt_count + 1)
        OR (OLD.state = 'claimed' AND NEW.state = 'delivery-attempted'
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
        OR (OLD.state = 'claimed' AND NEW.state = 'retry-wait')
        OR (OLD.state = 'delivery-attempted'
          AND NEW.state IN ('provider-started', 'ambiguous'))
        OR (OLD.state = 'ambiguous' AND NEW.state = 'provider-started'
          AND NEW.provider_turn_id IS NOT NULL AND NEW.provider_accepted_at IS NOT NULL)
      )
    BEGIN SELECT RAISE(ABORT, 'invalid verification delivery transition'); END
  `;
});

/** Durable Verification materialization, provider delivery, and provider-start evidence. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const alreadyApplied = yield* sql<{ readonly count: number }>`
    SELECT count(*) AS count FROM sqlite_schema
    WHERE type = 'table' AND name = 'agent_control_verification_stage_started_markers'
  `;
  if (alreadyApplied[0]?.count === 1) return;

  yield* sql`PRAGMA defer_foreign_keys = ON`;
  yield* rebuildAgentControlEvents;
  yield* rebuildOrchestrationMaterializationIntents;
  yield* rebuildVerificationReservation;
  yield* expandVerificationStageBoundary;
  yield* createVerificationEvidence;

  const violations = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
  if (violations.length !== 0) {
    return yield* Effect.die(new Error("migration 058 introduced foreign-key violations"));
  }
});
