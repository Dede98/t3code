import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const quoteIdentifier = (value: string) => `"${value.replaceAll('"', '""')}"`;
const text = (column: string) =>
  `typeof(${column}) = 'text' AND length(${column}) > 0 AND trim(${column}) = ${column}`;
const positiveInteger = (column: string) => `typeof(${column}) = 'integer' AND ${column} >= 1`;
const sha256 = (column: string) =>
  `${text(column)} AND length(${column}) = 64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;
const timestamp = (column: string) => `
  ${text(column)}
  AND length(${column}) = 24
  AND ${column} GLOB
    '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
  AND strftime('%Y-%m-%dT%H:%M:%fZ', ${column}) = ${column}
`;
const every = (predicates: ReadonlyArray<string>) => predicates.join(" AND ");

const createStorageTrigger = (
  table: string,
  predicates: ReadonlyArray<string>,
  message: string,
) => `
  CREATE TRIGGER ${table}_storage_validate
  BEFORE INSERT ON ${table}
  WHEN NOT COALESCE((${every(predicates)}), 0)
  BEGIN SELECT RAISE(ABORT, '${message}'); END
`;

const planningOnlyTrigger = (sql: string) => {
  const needle = "WHEN NEW.aggregate_kind = 'controlled-thread-reservation'";
  if (!sql.includes(needle)) {
    throw new Error("migration 054 could not preserve the controlled-thread planning trigger");
  }
  return sql.replace(
    needle,
    `${needle}\n    AND json_extract(NEW.payload_json, '$.stageKind') = 'planning'`,
  );
};

/** Durable admission of the implementation@2 successor, without materialization. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE agent_control_implementation_thread_stream_catalog (
      controlled_thread_reservation_id TEXT PRIMARY KEY,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind = 'controlled-thread-reservation'),
      stream_version INTEGER NOT NULL CHECK (stream_version = 1),
      command_id TEXT NOT NULL,
      event_type TEXT NOT NULL CHECK (
        event_type = 'agentControl.controlledThreadReservation.prepared'
      ),
      thread_id TEXT NOT NULL UNIQUE CHECK (thread_id LIKE 't3-auto-reserved-thread-%'),
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
      role_id TEXT NOT NULL CHECK (role_id = 'implementer'),
      stage_kind TEXT NOT NULL CHECK (stage_kind = 'implementation'),
      stage_ordinal INTEGER NOT NULL CHECK (stage_ordinal = 2),
      attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal = 1),
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 2),
      worktree_reservation_id TEXT NOT NULL UNIQUE,
      prepared_at TEXT NOT NULL,
      UNIQUE (
        project_id, task_id, task_revision, github_intake_sequence,
        source_identity_fingerprint, stage_kind, stage_ordinal, attempt_ordinal
      ),
      FOREIGN KEY (stage_run_id)
        REFERENCES agent_control_stage_run_states(stage_run_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (lease_id)
        REFERENCES agent_control_stage_run_lease_states(lease_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (worktree_reservation_id)
        REFERENCES agent_control_worktree_reservation_states(reservation_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (
        event_id, aggregate_kind, controlled_thread_reservation_id,
        stream_version, event_type, command_id
      ) REFERENCES agent_control_events(
        event_id, aggregate_kind, stream_id,
        stream_version, event_type, command_id
      ) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_implementation_thread_reservation_states (
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
      role_id TEXT NOT NULL CHECK (role_id = 'implementer'),
      stage_kind TEXT NOT NULL CHECK (stage_kind = 'implementation'),
      stage_ordinal INTEGER NOT NULL CHECK (stage_ordinal = 2),
      attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal = 1),
      lease_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 2),
      worktree_reservation_id TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL CHECK (status = 'prepared'),
      revision INTEGER NOT NULL CHECK (revision = 1),
      last_event_sequence INTEGER NOT NULL UNIQUE CHECK (last_event_sequence >= 1),
      prepared_at TEXT NOT NULL,
      state_json TEXT NOT NULL CHECK (
        json_valid(state_json) = 1 AND json_type(state_json) = 'object'
      ),
      UNIQUE (
        project_id, task_id, stage_run_id, attempt_id, role_id,
        stage_ordinal, attempt_ordinal
      ),
      FOREIGN KEY (controlled_thread_reservation_id)
        REFERENCES agent_control_implementation_thread_stream_catalog(
          controlled_thread_reservation_id
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (last_event_sequence)
        REFERENCES agent_control_events(sequence) ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql.unsafe(
    createStorageTrigger(
      "agent_control_implementation_thread_stream_catalog",
      [
        ...[
          "controlled_thread_reservation_id",
          "event_id",
          "aggregate_kind",
          "command_id",
          "event_type",
          "thread_id",
          "project_id",
          "task_id",
          "stage_run_id",
          "attempt_id",
          "role_id",
          "stage_kind",
          "lease_id",
          "worktree_reservation_id",
        ].map((column) => text(`NEW.${column}`)),
        positiveInteger("NEW.stream_version"),
        positiveInteger("NEW.task_revision"),
        positiveInteger("NEW.github_intake_sequence"),
        sha256("NEW.source_identity_fingerprint"),
        positiveInteger("NEW.stage_ordinal"),
        positiveInteger("NEW.attempt_ordinal"),
        positiveInteger("NEW.fence_token"),
        timestamp("NEW.prepared_at"),
      ],
      "invalid implementation reservation catalog storage",
    ),
  ).unprepared;
  yield* sql.unsafe(
    createStorageTrigger(
      "agent_control_implementation_thread_reservation_states",
      [
        ...[
          "controlled_thread_reservation_id",
          "thread_id",
          "project_id",
          "task_id",
          "stage_run_id",
          "attempt_id",
          "role_id",
          "stage_kind",
          "lease_id",
          "worktree_reservation_id",
          "status",
        ].map((column) => text(`NEW.${column}`)),
        positiveInteger("NEW.task_revision"),
        positiveInteger("NEW.github_intake_sequence"),
        sha256("NEW.source_identity_fingerprint"),
        positiveInteger("NEW.stage_ordinal"),
        positiveInteger("NEW.attempt_ordinal"),
        positiveInteger("NEW.fence_token"),
        positiveInteger("NEW.revision"),
        positiveInteger("NEW.last_event_sequence"),
        timestamp("NEW.prepared_at"),
        `${text("NEW.state_json")} AND json_valid(NEW.state_json) = 1`,
      ],
      "invalid implementation reservation projection storage",
    ),
  ).unprepared;

  yield* sql`
    CREATE VIEW agent_control_controlled_thread_stream_catalog_all AS
    SELECT * FROM agent_control_controlled_thread_stream_catalog
    UNION ALL
    SELECT
      controlled_thread_reservation_id, event_id, aggregate_kind, stream_version,
      command_id, event_type, thread_id, project_id, task_id, task_revision,
      github_intake_sequence, source_identity_fingerprint, stage_run_id, attempt_id,
      role_id, stage_kind, stage_ordinal, attempt_ordinal, lease_id, fence_token,
      worktree_reservation_id, prepared_at,
      NULL AS coordinator_command_id, NULL AS coordinator_command_fingerprint,
      NULL AS materializing_transition_command_id, NULL AS materialization_command_id,
      NULL AS materialization_command_fingerprint, NULL AS lease_holder_id,
      NULL AS materializing_at, NULL AS bound_transition_command_id,
      NULL AS orchestration_result_sequence, NULL AS materialized_at, NULL AS bound_at
    FROM agent_control_implementation_thread_stream_catalog
  `;

  yield* sql`
    CREATE VIEW agent_control_controlled_thread_reservation_states_all AS
    SELECT * FROM agent_control_controlled_thread_reservation_states
    UNION ALL
    SELECT
      controlled_thread_reservation_id, thread_id, project_id, task_id, task_revision,
      github_intake_sequence, source_identity_fingerprint, stage_run_id, attempt_id,
      role_id, stage_kind, stage_ordinal, attempt_ordinal, lease_id, fence_token,
      worktree_reservation_id, status, revision, last_event_sequence, prepared_at,
      NULL AS coordinator_command_id, NULL AS coordinator_command_fingerprint,
      NULL AS materializing_transition_command_id, NULL AS materialization_command_id,
      NULL AS materialization_command_fingerprint, NULL AS lease_holder_id,
      NULL AS materializing_at, NULL AS bound_transition_command_id,
      NULL AS orchestration_result_sequence, NULL AS materialized_at, NULL AS bound_at,
      state_json
    FROM agent_control_implementation_thread_reservation_states
  `;

  const planningTriggers = yield* sql<{ readonly name: string; readonly sql: string }>`
    SELECT name, sql FROM sqlite_schema
    WHERE type = 'trigger' AND name IN (
      'agent_control_controlled_thread_event_validate',
      'agent_control_controlled_thread_event_json_total_validate'
    )
    ORDER BY name
  `;
  if (planningTriggers.length !== 2) {
    return yield* Effect.die(new Error("migration 054 requires both planning event guards"));
  }
  for (const trigger of planningTriggers) {
    yield* sql.unsafe(`DROP TRIGGER ${quoteIdentifier(trigger.name)}`).unprepared;
    const statement = planningOnlyTrigger(trigger.sql);
    yield* sql.unsafe(statement).unprepared;
  }

  yield* sql`
    CREATE TRIGGER agent_control_implementation_thread_catalog_validate
    BEFORE INSERT ON agent_control_implementation_thread_stream_catalog
    BEGIN
      SELECT CASE WHEN EXISTS (
        SELECT 1 FROM agent_control_controlled_thread_stream_catalog old
        WHERE old.controlled_thread_reservation_id = NEW.controlled_thread_reservation_id
          OR old.thread_id = NEW.thread_id
      ) THEN RAISE(ABORT, 'implementation reservation conflicts with planning history') END;
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM agent_control_stage_run_states stage
        WHERE stage.stage_run_id = NEW.stage_run_id
          AND stage.project_id = NEW.project_id AND stage.task_id = NEW.task_id
          AND stage.attempt_id = NEW.attempt_id AND stage.role_id = 'implementer'
          AND stage.stage_kind = 'implementation' AND stage.stage_ordinal = 2
          AND stage.attempt_ordinal = 1 AND stage.status = 'prepared'
          AND stage.revision = 1 AND stage.task_revision = NEW.task_revision
          AND stage.github_intake_sequence = NEW.github_intake_sequence
          AND stage.source_identity_fingerprint = NEW.source_identity_fingerprint
      ) THEN RAISE(ABORT, 'implementation reservation stage binding is invalid') END;
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM agent_control_stage_run_lease_states lease
        WHERE lease.lease_id = NEW.lease_id AND lease.project_id = NEW.project_id
          AND lease.task_id = NEW.task_id AND lease.stage_run_id = NEW.stage_run_id
          AND lease.attempt_id = NEW.attempt_id AND lease.status = 'reserved'
          AND lease.fence_token = NEW.fence_token
          AND lease.task_revision = NEW.task_revision
          AND lease.github_intake_sequence = NEW.github_intake_sequence
          AND lease.source_identity_fingerprint = NEW.source_identity_fingerprint
      ) THEN RAISE(ABORT, 'implementation reservation lease binding is invalid') END;
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM agent_control_worktree_reservation_states worktree
        WHERE worktree.reservation_id = NEW.worktree_reservation_id
          AND worktree.project_id = NEW.project_id AND worktree.task_id = NEW.task_id
          AND worktree.task_revision = NEW.task_revision
          AND worktree.github_intake_sequence = NEW.github_intake_sequence
          AND worktree.source_identity_fingerprint = NEW.source_identity_fingerprint
          AND worktree.status = 'ready' AND worktree.verified_at IS NOT NULL
          AND worktree.ownership_fingerprint IS NOT NULL
      ) THEN RAISE(ABORT, 'implementation reservation worktree binding is invalid') END;
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_implementation_thread_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'controlled-thread-reservation'
      AND json_extract(NEW.payload_json, '$.stageKind') = 'implementation'
    BEGIN
      SELECT CASE WHEN NOT (
        typeof(NEW.event_id) = 'text' AND length(NEW.event_id) > 0
        AND NEW.stream_version = 1
        AND NEW.event_type = 'agentControl.controlledThreadReservation.prepared'
        AND NEW.actor_authority = 'controller' AND NEW.causation_event_id IS NULL
        AND NEW.command_id = NEW.correlation_id
      ) THEN RAISE(ABORT, 'implementation reservation event header is invalid') END;
      SELECT CASE WHEN NOT (
        NEW.metadata_json = '{"schemaVersion":1}'
        AND json_valid(NEW.payload_json) = 1
        AND json_type(NEW.payload_json) = 'object'
        AND (SELECT count(*) FROM json_each(NEW.payload_json)) = 18
      ) THEN RAISE(ABORT, 'implementation reservation event json is invalid') END;
      SELECT CASE WHEN NOT (
        json_extract(NEW.payload_json, '$.roleId') = 'implementer'
        AND json_extract(NEW.payload_json, '$.stageKind') = 'implementation'
        AND json_extract(NEW.payload_json, '$.stageOrdinal') = 2
        AND json_extract(NEW.payload_json, '$.attemptOrdinal') = 1
        AND json_extract(NEW.payload_json, '$.status') = 'prepared'
        AND NEW.stream_id = json_extract(NEW.payload_json, '$.controlledThreadReservationId')
        AND NEW.occurred_at = json_extract(NEW.payload_json, '$.preparedAt')
        AND strftime('%Y-%m-%dT%H:%M:%fZ', NEW.occurred_at) = NEW.occurred_at
      ) THEN RAISE(ABORT, 'implementation reservation event identity is invalid') END;
      SELECT CASE WHEN NEW.payload_json <> json_object(
          'controlledThreadReservationId', json_extract(NEW.payload_json, '$.controlledThreadReservationId'),
          'threadId', json_extract(NEW.payload_json, '$.threadId'),
          'projectId', json_extract(NEW.payload_json, '$.projectId'),
          'taskId', json_extract(NEW.payload_json, '$.taskId'),
          'taskRevision', json_extract(NEW.payload_json, '$.taskRevision'),
          'githubIntakeSequence', json_extract(NEW.payload_json, '$.githubIntakeSequence'),
          'sourceIdentityFingerprint', json_extract(NEW.payload_json, '$.sourceIdentityFingerprint'),
          'stageRunId', json_extract(NEW.payload_json, '$.stageRunId'),
          'attemptId', json_extract(NEW.payload_json, '$.attemptId'),
          'roleId', json_extract(NEW.payload_json, '$.roleId'),
          'stageKind', json_extract(NEW.payload_json, '$.stageKind'),
          'stageOrdinal', json_extract(NEW.payload_json, '$.stageOrdinal'),
          'attemptOrdinal', json_extract(NEW.payload_json, '$.attemptOrdinal'),
          'leaseId', json_extract(NEW.payload_json, '$.leaseId'),
          'fenceToken', json_extract(NEW.payload_json, '$.fenceToken'),
          'worktreeReservationId', json_extract(NEW.payload_json, '$.worktreeReservationId'),
          'status', json_extract(NEW.payload_json, '$.status'),
          'preparedAt', json_extract(NEW.payload_json, '$.preparedAt')
        ) THEN RAISE(ABORT, 'implementation reservation event is noncanonical') END;
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM agent_control_implementation_thread_stream_catalog catalog
        WHERE catalog.controlled_thread_reservation_id = NEW.stream_id
          AND catalog.event_id = NEW.event_id AND catalog.command_id = NEW.command_id
          AND catalog.thread_id = json_extract(NEW.payload_json, '$.threadId')
          AND catalog.project_id = json_extract(NEW.payload_json, '$.projectId')
          AND catalog.task_id = json_extract(NEW.payload_json, '$.taskId')
          AND catalog.task_revision = json_extract(NEW.payload_json, '$.taskRevision')
          AND catalog.github_intake_sequence =
            json_extract(NEW.payload_json, '$.githubIntakeSequence')
          AND catalog.source_identity_fingerprint =
            json_extract(NEW.payload_json, '$.sourceIdentityFingerprint')
          AND catalog.stage_run_id = json_extract(NEW.payload_json, '$.stageRunId')
          AND catalog.attempt_id = json_extract(NEW.payload_json, '$.attemptId')
          AND catalog.lease_id = json_extract(NEW.payload_json, '$.leaseId')
          AND catalog.fence_token = json_extract(NEW.payload_json, '$.fenceToken')
          AND catalog.worktree_reservation_id =
            json_extract(NEW.payload_json, '$.worktreeReservationId')
          AND catalog.prepared_at = json_extract(NEW.payload_json, '$.preparedAt')
      ) THEN
        RAISE(ABORT, 'implementation reservation event catalog binding is invalid') END;
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_implementation_thread_projection_validate
    BEFORE INSERT ON agent_control_implementation_thread_reservation_states
    BEGIN
      SELECT CASE WHEN NOT (
        NEW.status = 'prepared' AND NEW.revision = 1
        AND NEW.role_id = 'implementer' AND NEW.stage_kind = 'implementation'
        AND NEW.stage_ordinal = 2 AND NEW.attempt_ordinal = 1
      ) THEN RAISE(ABORT, 'implementation reservation projection identity is invalid') END;
      SELECT CASE WHEN NEW.state_json <> json_object(
          'schemaVersion', 1,
          'controlledThreadReservationId', NEW.controlled_thread_reservation_id,
          'threadId', NEW.thread_id, 'projectId', NEW.project_id, 'taskId', NEW.task_id,
          'taskRevision', NEW.task_revision,
          'githubIntakeSequence', NEW.github_intake_sequence,
          'sourceIdentityFingerprint', NEW.source_identity_fingerprint,
          'stageRunId', NEW.stage_run_id, 'attemptId', NEW.attempt_id,
          'roleId', NEW.role_id, 'stageKind', NEW.stage_kind,
          'stageOrdinal', NEW.stage_ordinal, 'attemptOrdinal', NEW.attempt_ordinal,
          'leaseId', NEW.lease_id, 'fenceToken', NEW.fence_token,
          'worktreeReservationId', NEW.worktree_reservation_id,
          'status', NEW.status, 'revision', NEW.revision,
          'sequence', NEW.last_event_sequence, 'preparedAt', NEW.prepared_at
        ) THEN RAISE(ABORT, 'implementation reservation projection is noncanonical') END;
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM agent_control_implementation_thread_stream_catalog catalog
        JOIN agent_control_events event ON event.event_id = catalog.event_id
        WHERE catalog.controlled_thread_reservation_id = NEW.controlled_thread_reservation_id
          AND catalog.thread_id = NEW.thread_id AND catalog.project_id = NEW.project_id
          AND catalog.task_id = NEW.task_id AND catalog.stage_run_id = NEW.stage_run_id
          AND catalog.attempt_id = NEW.attempt_id AND catalog.lease_id = NEW.lease_id
          AND catalog.fence_token = NEW.fence_token
          AND catalog.worktree_reservation_id = NEW.worktree_reservation_id
          AND event.sequence = NEW.last_event_sequence
      ) THEN
        RAISE(ABORT, 'implementation reservation projection event binding is invalid') END;
    END
  `;

  for (const table of [
    "agent_control_implementation_thread_stream_catalog",
    "agent_control_implementation_thread_reservation_states",
  ]) {
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
       BEGIN SELECT RAISE(ABORT, 'implementation reservation evidence is immutable'); END`,
    ).unprepared;
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table}
       BEGIN SELECT RAISE(ABORT, 'implementation reservation evidence is immutable'); END`,
    ).unprepared;
  }

  yield* sql`
    CREATE TABLE agent_control_implementation_admission_evidence (
      admission_evidence_id TEXT PRIMARY KEY,
      admission_command_id TEXT NOT NULL UNIQUE,
      admission_fingerprint TEXT NOT NULL UNIQUE CHECK (
        length(admission_fingerprint) = 64 AND admission_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      result_evidence_id TEXT NOT NULL UNIQUE,
      planning_finalization_command_id TEXT NOT NULL UNIQUE,
      planning_finalization_fingerprint TEXT NOT NULL UNIQUE,
      planning_marker_id TEXT NOT NULL UNIQUE,
      planning_marker_fingerprint TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL UNIQUE,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        length(source_identity_fingerprint) = 64
        AND source_identity_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      planning_stage_run_id TEXT NOT NULL UNIQUE,
      planning_attempt_id TEXT NOT NULL UNIQUE,
      planning_thread_id TEXT NOT NULL UNIQUE,
      planning_controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      provider_delivery_id TEXT NOT NULL UNIQUE,
      provider_instance_id TEXT NOT NULL,
      provider_turn_id TEXT NOT NULL,
      runtime_mode TEXT NOT NULL CHECK (runtime_mode IN ('approval-required', 'full-access')),
      model_selection_fingerprint TEXT NOT NULL,
      orchestration_started_event_id TEXT NOT NULL UNIQUE,
      orchestration_started_sequence INTEGER NOT NULL UNIQUE CHECK (
        orchestration_started_sequence >= 1
      ),
      orchestration_terminal_event_id TEXT NOT NULL UNIQUE,
      orchestration_terminal_sequence INTEGER NOT NULL UNIQUE CHECK (
        orchestration_terminal_sequence > orchestration_started_sequence
      ),
      planning_stage_event_id TEXT NOT NULL UNIQUE,
      planning_stage_event_sequence INTEGER NOT NULL UNIQUE CHECK (
        planning_stage_event_sequence >= 1
      ),
      planning_lease_id TEXT NOT NULL,
      planning_lease_holder_id TEXT NOT NULL,
      planning_fence_token INTEGER NOT NULL CHECK (planning_fence_token >= 1),
      planning_lease_release_event_id TEXT NOT NULL UNIQUE,
      planning_lease_release_event_sequence INTEGER NOT NULL UNIQUE CHECK (
        planning_lease_release_event_sequence >= 1
      ),
      planning_lease_release_stream_version INTEGER NOT NULL CHECK (
        planning_lease_release_stream_version >= 2
      ),
      planning_finalized_at TEXT NOT NULL,
      worktree_reservation_id TEXT NOT NULL UNIQUE,
      worktree_revision INTEGER NOT NULL CHECK (worktree_revision >= 1),
      worktree_event_sequence INTEGER NOT NULL UNIQUE CHECK (worktree_event_sequence >= 1),
      worktree_ownership_fingerprint TEXT NOT NULL CHECK (
        length(worktree_ownership_fingerprint) = 64
        AND worktree_ownership_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      worktree_verified_at TEXT NOT NULL,
      plan_id TEXT NOT NULL UNIQUE,
      plan_event_id TEXT NOT NULL UNIQUE,
      plan_event_sequence INTEGER NOT NULL UNIQUE CHECK (plan_event_sequence >= 1),
      proposed_plan_json TEXT NOT NULL CHECK (
        json_valid(proposed_plan_json) = 1 AND json_type(proposed_plan_json) = 'object'
      ),
      proposed_plan_digest TEXT NOT NULL UNIQUE CHECK (
        length(proposed_plan_digest) = 64 AND proposed_plan_digest NOT GLOB '*[^0-9a-f]*'
      ),
      implementation_stage_run_id TEXT NOT NULL UNIQUE,
      implementation_attempt_id TEXT NOT NULL UNIQUE,
      implementation_stage_event_id TEXT NOT NULL UNIQUE,
      implementation_stage_event_sequence INTEGER NOT NULL UNIQUE CHECK (
        implementation_stage_event_sequence >= 1
      ),
      implementation_lease_id TEXT NOT NULL,
      implementation_lease_holder_id TEXT NOT NULL,
      implementation_fence_token INTEGER NOT NULL CHECK (implementation_fence_token >= 2),
      implementation_lease_event_id TEXT NOT NULL UNIQUE,
      implementation_lease_event_sequence INTEGER NOT NULL UNIQUE CHECK (
        implementation_lease_event_sequence >= 1
      ),
      implementation_lease_event_stream_version INTEGER NOT NULL CHECK (
        implementation_lease_event_stream_version >= 3
      ),
      implementation_controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
      implementation_thread_id TEXT NOT NULL UNIQUE,
      implementation_reservation_event_id TEXT NOT NULL UNIQUE,
      implementation_reservation_event_sequence INTEGER NOT NULL UNIQUE CHECK (
        implementation_reservation_event_sequence >= 1
      ),
      admitted_at TEXT NOT NULL,
      CHECK (implementation_lease_id = planning_lease_id),
      CHECK (implementation_fence_token = planning_fence_token + 1),
      FOREIGN KEY (result_evidence_id)
        REFERENCES agent_control_initial_planning_result_evidence(result_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (planning_marker_id)
        REFERENCES agent_control_initial_planning_finalization_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (worktree_reservation_id)
        REFERENCES agent_control_worktree_reservation_states(reservation_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (implementation_stage_event_id)
        REFERENCES agent_control_events(event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (implementation_lease_event_id, implementation_lease_id,
        implementation_lease_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (implementation_reservation_event_id)
        REFERENCES agent_control_events(event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_implementation_admission_receipts (
      receipt_id TEXT PRIMARY KEY,
      admission_command_id TEXT NOT NULL UNIQUE,
      admission_fingerprint TEXT NOT NULL UNIQUE,
      admission_evidence_id TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL UNIQUE,
      implementation_stage_event_id TEXT NOT NULL UNIQUE,
      implementation_stage_event_sequence INTEGER NOT NULL UNIQUE,
      implementation_lease_event_id TEXT NOT NULL UNIQUE,
      implementation_lease_event_sequence INTEGER NOT NULL UNIQUE,
      implementation_reservation_event_id TEXT NOT NULL UNIQUE,
      implementation_reservation_event_sequence INTEGER NOT NULL UNIQUE,
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (admission_evidence_id)
        REFERENCES agent_control_implementation_admission_evidence(admission_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_implementation_admission_markers (
      marker_id TEXT PRIMARY KEY,
      marker_fingerprint TEXT NOT NULL UNIQUE CHECK (
        length(marker_fingerprint) = 64 AND marker_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      admission_command_id TEXT NOT NULL UNIQUE,
      admission_evidence_id TEXT NOT NULL UNIQUE,
      receipt_id TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL UNIQUE,
      committed_at TEXT NOT NULL,
      FOREIGN KEY (admission_evidence_id)
        REFERENCES agent_control_implementation_admission_evidence(admission_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (receipt_id)
        REFERENCES agent_control_implementation_admission_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql.unsafe(
    createStorageTrigger(
      "agent_control_implementation_admission_evidence",
      [
        ...[
          "admission_evidence_id",
          "admission_command_id",
          "result_evidence_id",
          "planning_finalization_command_id",
          "planning_marker_id",
          "handoff_id",
          "project_id",
          "task_id",
          "planning_stage_run_id",
          "planning_attempt_id",
          "planning_thread_id",
          "planning_controlled_thread_reservation_id",
          "provider_delivery_id",
          "provider_instance_id",
          "provider_turn_id",
          "runtime_mode",
          "orchestration_started_event_id",
          "orchestration_terminal_event_id",
          "planning_stage_event_id",
          "planning_lease_id",
          "planning_lease_holder_id",
          "planning_lease_release_event_id",
          "worktree_reservation_id",
          "plan_id",
          "plan_event_id",
          "proposed_plan_json",
          "implementation_stage_run_id",
          "implementation_attempt_id",
          "implementation_stage_event_id",
          "implementation_lease_id",
          "implementation_lease_holder_id",
          "implementation_lease_event_id",
          "implementation_controlled_thread_reservation_id",
          "implementation_thread_id",
          "implementation_reservation_event_id",
        ].map((column) => text(`NEW.${column}`)),
        ...[
          "admission_fingerprint",
          "planning_finalization_fingerprint",
          "planning_marker_fingerprint",
          "source_identity_fingerprint",
          "model_selection_fingerprint",
          "proposed_plan_digest",
          "worktree_ownership_fingerprint",
        ].map((column) => sha256(`NEW.${column}`)),
        ...[
          "task_revision",
          "github_intake_sequence",
          "orchestration_started_sequence",
          "orchestration_terminal_sequence",
          "planning_stage_event_sequence",
          "planning_fence_token",
          "planning_lease_release_event_sequence",
          "planning_lease_release_stream_version",
          "worktree_revision",
          "worktree_event_sequence",
          "plan_event_sequence",
          "implementation_stage_event_sequence",
          "implementation_fence_token",
          "implementation_lease_event_sequence",
          "implementation_lease_event_stream_version",
          "implementation_reservation_event_sequence",
        ].map((column) => positiveInteger(`NEW.${column}`)),
        timestamp("NEW.planning_finalized_at"),
        timestamp("NEW.worktree_verified_at"),
        timestamp("NEW.admitted_at"),
        "json_valid(NEW.proposed_plan_json) = 1",
      ],
      "invalid implementation admission evidence storage",
    ),
  ).unprepared;
  yield* sql.unsafe(
    createStorageTrigger(
      "agent_control_implementation_admission_receipts",
      [
        ...[
          "receipt_id",
          "admission_command_id",
          "admission_evidence_id",
          "handoff_id",
          "implementation_stage_event_id",
          "implementation_lease_event_id",
          "implementation_reservation_event_id",
        ].map((column) => text(`NEW.${column}`)),
        sha256("NEW.admission_fingerprint"),
        positiveInteger("NEW.implementation_stage_event_sequence"),
        positiveInteger("NEW.implementation_lease_event_sequence"),
        positiveInteger("NEW.implementation_reservation_event_sequence"),
        timestamp("NEW.accepted_at"),
      ],
      "invalid implementation admission receipt storage",
    ),
  ).unprepared;
  yield* sql.unsafe(
    createStorageTrigger(
      "agent_control_implementation_admission_markers",
      [
        ...[
          "marker_id",
          "admission_command_id",
          "admission_evidence_id",
          "receipt_id",
          "handoff_id",
        ].map((column) => text(`NEW.${column}`)),
        sha256("NEW.marker_fingerprint"),
        timestamp("NEW.committed_at"),
      ],
      "invalid implementation admission marker storage",
    ),
  ).unprepared;

  yield* sql`
    CREATE TRIGGER agent_control_implementation_admission_evidence_validate
    BEFORE INSERT ON agent_control_implementation_admission_evidence
    BEGIN
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM agent_control_initial_planning_result_evidence result
        JOIN agent_control_initial_planning_finalization_receipts receipt
          ON receipt.result_evidence_id = result.result_evidence_id
        JOIN agent_control_initial_planning_finalization_markers marker
          ON marker.result_evidence_id = result.result_evidence_id
        JOIN agent_control_controlled_thread_reservation_states planning_reservation
          ON planning_reservation.controlled_thread_reservation_id =
            result.controlled_thread_reservation_id
        JOIN agent_control_worktree_reservation_states worktree
          ON worktree.reservation_id = NEW.worktree_reservation_id
        WHERE result.result_evidence_id = NEW.result_evidence_id
          AND result.outcome = 'succeeded'
          AND result.finalization_command_id = NEW.planning_finalization_command_id
          AND result.finalization_fingerprint = NEW.planning_finalization_fingerprint
          AND marker.marker_id = NEW.planning_marker_id
          AND marker.marker_fingerprint = NEW.planning_marker_fingerprint
          AND result.handoff_id = NEW.handoff_id
          AND result.project_id = NEW.project_id AND result.task_id = NEW.task_id
          AND result.task_revision = NEW.task_revision
          AND result.github_intake_sequence = NEW.github_intake_sequence
          AND result.source_identity_fingerprint = NEW.source_identity_fingerprint
          AND result.stage_run_id = NEW.planning_stage_run_id
          AND result.attempt_id = NEW.planning_attempt_id
          AND result.thread_id = NEW.planning_thread_id
          AND result.controlled_thread_reservation_id =
            NEW.planning_controlled_thread_reservation_id
          AND result.provider_delivery_id = NEW.provider_delivery_id
          AND result.provider_instance_id = NEW.provider_instance_id
          AND result.provider_turn_id = NEW.provider_turn_id
          AND result.runtime_mode = NEW.runtime_mode
          AND result.model_selection_fingerprint = NEW.model_selection_fingerprint
          AND result.orchestration_started_event_id = NEW.orchestration_started_event_id
          AND result.orchestration_started_sequence = NEW.orchestration_started_sequence
          AND result.orchestration_terminal_event_id = NEW.orchestration_terminal_event_id
          AND result.orchestration_terminal_sequence = NEW.orchestration_terminal_sequence
          AND result.stage_event_id = NEW.planning_stage_event_id
          AND result.stage_event_sequence = NEW.planning_stage_event_sequence
          AND result.lease_id = NEW.planning_lease_id
          AND result.lease_holder_id = NEW.planning_lease_holder_id
          AND result.fence_token = NEW.planning_fence_token
          AND result.lease_event_id = NEW.planning_lease_release_event_id
          AND result.lease_event_sequence = NEW.planning_lease_release_event_sequence
          AND result.lease_event_stream_version =
            NEW.planning_lease_release_stream_version
          AND result.finalized_at = NEW.planning_finalized_at
          AND result.plan_id = NEW.plan_id AND result.plan_event_id = NEW.plan_event_id
          AND result.plan_event_sequence = NEW.plan_event_sequence
          AND CAST(result.proposed_plan_json AS BLOB) = CAST(NEW.proposed_plan_json AS BLOB)
          AND result.proposed_plan_digest = NEW.proposed_plan_digest
          AND planning_reservation.thread_id = NEW.planning_thread_id
          AND planning_reservation.status = 'bound' AND planning_reservation.revision = 3
          AND planning_reservation.worktree_reservation_id = NEW.worktree_reservation_id
          AND worktree.status = 'ready' AND worktree.project_id = NEW.project_id
          AND worktree.task_id = NEW.task_id AND worktree.task_revision = NEW.task_revision
          AND worktree.github_intake_sequence = NEW.github_intake_sequence
          AND worktree.source_identity_fingerprint = NEW.source_identity_fingerprint
          AND worktree.revision = NEW.worktree_revision
          AND worktree.last_event_sequence = NEW.worktree_event_sequence
          AND worktree.ownership_fingerprint = NEW.worktree_ownership_fingerprint
          AND worktree.verified_at = NEW.worktree_verified_at
      ) THEN RAISE(ABORT, 'implementation admission planning evidence is inconsistent') END;
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM agent_control_events stage
        JOIN agent_control_events lease ON lease.event_id = NEW.implementation_lease_event_id
        JOIN agent_control_events reservation
          ON reservation.event_id = NEW.implementation_reservation_event_id
        JOIN agent_control_implementation_thread_reservation_states projection
          ON projection.controlled_thread_reservation_id =
            NEW.implementation_controlled_thread_reservation_id
        WHERE stage.event_id = NEW.implementation_stage_event_id
          AND stage.sequence = NEW.implementation_stage_event_sequence
          AND stage.stream_id = NEW.implementation_stage_run_id
          AND stage.stream_version = 1
          AND stage.event_type = 'agentControl.stageRun.prepared'
          AND json_extract(stage.payload_json, '$.attemptId') = NEW.implementation_attempt_id
          AND json_extract(stage.payload_json, '$.roleId') = 'implementer'
          AND json_extract(stage.payload_json, '$.stageKind') = 'implementation'
          AND json_extract(stage.payload_json, '$.stageOrdinal') = 2
          AND json_extract(stage.payload_json, '$.attemptOrdinal') = 1
          AND lease.sequence = NEW.implementation_lease_event_sequence
          AND lease.stream_id = NEW.implementation_lease_id
          AND lease.stream_version = NEW.implementation_lease_event_stream_version
          AND lease.event_type = 'agentControl.stageRunLease.reserved'
          AND json_extract(lease.payload_json, '$.stageRunId') =
            NEW.implementation_stage_run_id
          AND json_extract(lease.payload_json, '$.attemptId') = NEW.implementation_attempt_id
          AND json_extract(lease.payload_json, '$.holderId') =
            NEW.implementation_lease_holder_id
          AND json_extract(lease.payload_json, '$.fenceToken') =
            NEW.implementation_fence_token
          AND reservation.sequence = NEW.implementation_reservation_event_sequence
          AND reservation.stream_id = NEW.implementation_controlled_thread_reservation_id
          AND reservation.stream_version = 1
          AND reservation.event_type = 'agentControl.controlledThreadReservation.prepared'
          AND projection.stage_run_id = NEW.implementation_stage_run_id
          AND projection.attempt_id = NEW.implementation_attempt_id
          AND projection.thread_id = NEW.implementation_thread_id
          AND projection.lease_id = NEW.implementation_lease_id
          AND projection.fence_token = NEW.implementation_fence_token
          AND projection.worktree_reservation_id = NEW.worktree_reservation_id
          AND projection.status = 'prepared' AND projection.revision = 1
      ) THEN RAISE(ABORT, 'implementation admission successor evidence is inconsistent') END;
    END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_implementation_admission_receipt_validate
    BEFORE INSERT ON agent_control_implementation_admission_receipts
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_implementation_admission_evidence evidence
      WHERE evidence.admission_evidence_id = NEW.admission_evidence_id
        AND evidence.admission_command_id = NEW.admission_command_id
        AND evidence.admission_fingerprint = NEW.admission_fingerprint
        AND evidence.handoff_id = NEW.handoff_id
        AND evidence.implementation_stage_event_id = NEW.implementation_stage_event_id
        AND evidence.implementation_stage_event_sequence =
          NEW.implementation_stage_event_sequence
        AND evidence.implementation_lease_event_id = NEW.implementation_lease_event_id
        AND evidence.implementation_lease_event_sequence =
          NEW.implementation_lease_event_sequence
        AND evidence.implementation_reservation_event_id =
          NEW.implementation_reservation_event_id
        AND evidence.implementation_reservation_event_sequence =
          NEW.implementation_reservation_event_sequence
    )
    BEGIN SELECT RAISE(ABORT, 'implementation admission receipt is inconsistent'); END
  `;

  yield* sql`
    CREATE TRIGGER agent_control_implementation_admission_marker_validate
    BEFORE INSERT ON agent_control_implementation_admission_markers
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_implementation_admission_evidence evidence
      JOIN agent_control_implementation_admission_receipts receipt
        ON receipt.admission_evidence_id = evidence.admission_evidence_id
      WHERE evidence.admission_evidence_id = NEW.admission_evidence_id
        AND evidence.admission_command_id = NEW.admission_command_id
        AND evidence.handoff_id = NEW.handoff_id AND receipt.receipt_id = NEW.receipt_id
        AND receipt.admission_command_id = NEW.admission_command_id
        AND receipt.handoff_id = NEW.handoff_id
    )
    BEGIN SELECT RAISE(ABORT, 'implementation admission marker is inconsistent'); END
  `;

  for (const table of [
    "agent_control_implementation_admission_evidence",
    "agent_control_implementation_admission_receipts",
    "agent_control_implementation_admission_markers",
  ]) {
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
       BEGIN SELECT RAISE(ABORT, 'implementation admission evidence is immutable'); END`,
    ).unprepared;
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table}
       BEGIN SELECT RAISE(ABORT, 'implementation admission evidence is immutable'); END`,
    ).unprepared;
  }

  const violations = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
  if (violations.length !== 0) {
    return yield* Effect.die(new Error("migration 054 introduced foreign-key violations"));
  }
});
