import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const quoteSqliteIdentifier = (identifier: string): string =>
  `"${identifier.replaceAll('"', '""')}"`;
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

/**
 * Durable Initial Planning Stage finalization.
 *
 * The lifecycle events remain reconstructible from agent_control_events. The
 * four companion tables freeze provider-start, result, accepted-receipt, and
 * commit-marker coordinates without turning projections into authority.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const alreadyApplied = yield* sql<{ readonly count: number }>`
    SELECT count(*) AS count FROM sqlite_schema
    WHERE type = 'table'
      AND name = 'agent_control_initial_planning_finalization_markers'
  `;
  if (alreadyApplied[0]?.count === 1) return;

  const eventTriggers = yield* sql<{
    readonly name: string;
    readonly sql: string;
  }>`
    SELECT name, sql FROM sqlite_schema
    WHERE type = 'trigger' AND sql IS NOT NULL
      AND (tbl_name = 'agent_control_events' OR sql LIKE '%agent_control_events%')
    ORDER BY name ASC
  `;
  const sequenceRows = yield* sql<{ readonly seq: number }>`
    SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
  `;
  const eventSequence = sequenceRows[0]?.seq;

  yield* sql`PRAGMA defer_foreign_keys = ON`;
  for (const trigger of eventTriggers) {
    yield* sql.unsafe(`DROP TRIGGER ${quoteSqliteIdentifier(trigger.name)}`).unprepared;
  }
  yield* sql`
    CREATE TABLE agent_control_events_rebuild_052 (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      aggregate_kind TEXT NOT NULL CHECK (aggregate_kind IN (
        'project-controller', 'github-intake', 'task', 'stage-run',
        'stage-run-lease', 'worktree-reservation',
        'controlled-thread-reservation'
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
          'agentControl.stageRun.prepared',
          'agentControl.stageRun.planningStarted',
          'agentControl.stageRun.planningSucceeded',
          'agentControl.stageRun.planningFailed',
          'agentControl.stageRun.planningCancelled'
        ))
        OR (aggregate_kind = 'stage-run-lease' AND event_type IN (
          'agentControl.stageRunLease.reserved', 'agentControl.stageRunLease.renewed',
          'agentControl.stageRunLease.releasedBeforeExecution',
          'agentControl.stageRunLease.releasedAfterPlanning'
        ))
        OR (aggregate_kind = 'worktree-reservation' AND event_type IN (
          'agentControl.worktree.reserved',
          'agentControl.worktree.materializationStarted',
          'agentControl.worktree.ready',
          'agentControl.worktree.needsAttention'
        ))
        OR (
          aggregate_kind = 'controlled-thread-reservation'
          AND actor_authority = 'controller'
          AND (
            (stream_version = 1
              AND event_type = 'agentControl.controlledThreadReservation.prepared')
            OR (stream_version = 2
              AND event_type = 'agentControl.controlledThreadReservation.materializing')
            OR (stream_version = 3
              AND event_type = 'agentControl.controlledThreadReservation.bound')
          )
        )
      )
    )
  `;
  yield* sql`
    INSERT INTO agent_control_events_rebuild_052 (
      sequence, event_id, aggregate_kind, stream_id, stream_version,
      event_type, occurred_at, command_id, causation_event_id,
      correlation_id, actor_authority, payload_json, metadata_json
    )
    SELECT sequence, event_id, aggregate_kind, stream_id, stream_version,
      event_type, occurred_at, command_id, causation_event_id,
      correlation_id, actor_authority, payload_json, metadata_json
    FROM agent_control_events
  `;
  yield* sql`DROP TABLE agent_control_events`;
  yield* sql`ALTER TABLE agent_control_events_rebuild_052 RENAME TO agent_control_events`;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_stream_version
    ON agent_control_events(aggregate_kind, stream_id, stream_version)
  `;
  yield* sql`
    CREATE INDEX idx_agent_control_events_stream_sequence
    ON agent_control_events(aggregate_kind, stream_id, sequence)
  `;
  yield* sql`CREATE INDEX idx_agent_control_events_command_id ON agent_control_events(command_id)`;
  yield* sql`
    CREATE INDEX idx_agent_control_events_correlation_id
    ON agent_control_events(correlation_id)
  `;
  yield* sql`CREATE INDEX idx_agent_control_events_sequence ON agent_control_events(sequence)`;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_worktree_relational_identity
    ON agent_control_events(event_id, stream_id, stream_version, event_type)
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_controlled_thread_relational_identity
    ON agent_control_events(
      event_id, aggregate_kind, stream_id, stream_version, event_type, command_id
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_agent_control_events_initial_planning_relational_identity
    ON agent_control_events(event_id, stream_id, stream_version)
  `;
  if (eventSequence !== undefined) {
    yield* sql`
      DELETE FROM sqlite_sequence
      WHERE name IN ('agent_control_events', 'agent_control_events_rebuild_052')
    `;
    yield* sql`
      INSERT INTO sqlite_sequence(name, seq) VALUES ('agent_control_events', ${eventSequence})
    `;
  }
  for (const trigger of eventTriggers) {
    yield* sql.unsafe(trigger.sql).unprepared;
  }

  yield* sql`
    CREATE TABLE agent_control_initial_planning_stage_started (
      start_command_id TEXT PRIMARY KEY CHECK (${sql.literal(text("start_command_id"))}),
      start_fingerprint TEXT UNIQUE NOT NULL CHECK (${sql.literal(sha256("start_fingerprint"))}),
      handoff_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("handoff_id"))}),
      handoff_fingerprint TEXT UNIQUE NOT NULL CHECK (${sql.literal(sha256("handoff_fingerprint"))}),
      project_id TEXT NOT NULL CHECK (${sql.literal(text("project_id"))}),
      task_id TEXT NOT NULL CHECK (${sql.literal(text("task_id"))}),
      task_revision INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("task_revision"))}),
      github_intake_sequence INTEGER NOT NULL CHECK (
        ${sql.literal(positiveInteger("github_intake_sequence"))}
      ),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        ${sql.literal(sha256("source_identity_fingerprint"))}
      ),
      controlled_thread_reservation_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("controlled_thread_reservation_id"))}
      ),
      thread_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      stage_run_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("stage_run_id"))}),
      attempt_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("attempt_id"))}),
      lease_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("lease_id"))}),
      lease_holder_id TEXT NOT NULL CHECK (${sql.literal(text("lease_holder_id"))}),
      fence_token INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("fence_token"))}),
      provider_delivery_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("provider_delivery_id"))}),
      provider_instance_id TEXT NOT NULL CHECK (${sql.literal(text("provider_instance_id"))}),
      provider_turn_id TEXT NOT NULL CHECK (${sql.literal(text("provider_turn_id"))}),
      runtime_mode TEXT NOT NULL CHECK (
        runtime_mode IN ('approval-required', 'full-access')
      ),
      model_selection_fingerprint TEXT NOT NULL CHECK (
        ${sql.literal(sha256("model_selection_fingerprint"))}
      ),
      provider_accepted_at TEXT NOT NULL CHECK (${sql.literal(timestamp("provider_accepted_at"))}),
      delivery_revision INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("delivery_revision"))}),
      orchestration_started_event_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("orchestration_started_event_id"))}
      ),
      orchestration_started_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("orchestration_started_sequence"))}
      ),
      orchestration_started_stream_version INTEGER NOT NULL CHECK (
        typeof(orchestration_started_stream_version) = 'integer'
        AND orchestration_started_stream_version >= 0
      ),
      stage_event_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("stage_event_id"))}),
      stage_event_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("stage_event_sequence"))}
      ),
      stage_event_stream_version INTEGER NOT NULL CHECK (stage_event_stream_version = 2),
      recorded_at TEXT NOT NULL CHECK (${sql.literal(timestamp("recorded_at"))}),
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_initial_planning_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (provider_delivery_id)
        REFERENCES agent_control_initial_planning_deliveries(provider_delivery_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (orchestration_started_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (
        stage_event_id, stage_run_id, stage_event_stream_version
      ) REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_result_evidence (
      result_evidence_id TEXT PRIMARY KEY CHECK (${sql.literal(text("result_evidence_id"))}),
      finalization_command_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("finalization_command_id"))}
      ),
      finalization_fingerprint TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("finalization_fingerprint"))}
      ),
      outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'cancelled')),
      handoff_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("handoff_id"))}),
      handoff_fingerprint TEXT UNIQUE NOT NULL CHECK (${sql.literal(sha256("handoff_fingerprint"))}),
      project_id TEXT NOT NULL CHECK (${sql.literal(text("project_id"))}),
      task_id TEXT NOT NULL CHECK (${sql.literal(text("task_id"))}),
      task_revision INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("task_revision"))}),
      github_intake_sequence INTEGER NOT NULL CHECK (
        ${sql.literal(positiveInteger("github_intake_sequence"))}
      ),
      source_identity_fingerprint TEXT NOT NULL CHECK (
        ${sql.literal(sha256("source_identity_fingerprint"))}
      ),
      controlled_thread_reservation_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("controlled_thread_reservation_id"))}
      ),
      thread_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("thread_id"))}),
      stage_run_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("stage_run_id"))}),
      attempt_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("attempt_id"))}),
      lease_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("lease_id"))}),
      lease_holder_id TEXT NOT NULL CHECK (${sql.literal(text("lease_holder_id"))}),
      fence_token INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("fence_token"))}),
      provider_delivery_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("provider_delivery_id"))}),
      provider_instance_id TEXT NOT NULL CHECK (${sql.literal(text("provider_instance_id"))}),
      provider_turn_id TEXT NOT NULL CHECK (${sql.literal(text("provider_turn_id"))}),
      runtime_mode TEXT NOT NULL CHECK (runtime_mode IN ('approval-required', 'full-access')),
      model_selection_fingerprint TEXT NOT NULL CHECK (
        ${sql.literal(sha256("model_selection_fingerprint"))}
      ),
      delivery_terminal_state TEXT NOT NULL CHECK (
        delivery_terminal_state IN ('completed', 'failed', 'interrupted')
      ),
      delivery_revision INTEGER NOT NULL CHECK (${sql.literal(positiveInteger("delivery_revision"))}),
      terminal_at TEXT NOT NULL CHECK (${sql.literal(timestamp("terminal_at"))}),
      orchestration_started_event_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("orchestration_started_event_id"))}
      ),
      orchestration_started_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("orchestration_started_sequence"))}
      ),
      orchestration_terminal_event_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("orchestration_terminal_event_id"))}
      ),
      orchestration_terminal_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("orchestration_terminal_sequence"))}
      ),
      plan_id TEXT UNIQUE,
      plan_event_id TEXT UNIQUE,
      plan_event_sequence INTEGER UNIQUE,
      proposed_plan_json TEXT,
      proposed_plan_digest TEXT UNIQUE,
      stage_event_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("stage_event_id"))}),
      stage_event_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("stage_event_sequence"))}
      ),
      stage_event_stream_version INTEGER NOT NULL CHECK (stage_event_stream_version = 3),
      lease_event_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("lease_event_id"))}),
      lease_event_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("lease_event_sequence"))}
      ),
      lease_event_stream_version INTEGER NOT NULL CHECK (lease_event_stream_version >= 2),
      finalized_at TEXT NOT NULL CHECK (${sql.literal(timestamp("finalized_at"))}),
      CHECK (
        (outcome = 'succeeded'
          AND plan_id IS NOT NULL AND ${sql.literal(text("plan_id"))}
          AND plan_event_id IS NOT NULL AND ${sql.literal(text("plan_event_id"))}
          AND ${sql.literal(positiveInteger("plan_event_sequence"))}
          AND proposed_plan_json IS NOT NULL AND ${sql.literal(text("proposed_plan_json"))}
          AND json_valid(proposed_plan_json) = 1
          AND proposed_plan_digest IS NOT NULL AND ${sql.literal(sha256("proposed_plan_digest"))})
        OR
        (outcome IN ('failed', 'cancelled')
          AND plan_id IS NULL AND plan_event_id IS NULL AND plan_event_sequence IS NULL
          AND proposed_plan_json IS NULL AND proposed_plan_digest IS NULL)
      ),
      CHECK (
        (delivery_terminal_state = 'completed' AND outcome = 'succeeded')
        OR (delivery_terminal_state = 'failed' AND outcome = 'failed')
        OR (delivery_terminal_state = 'interrupted' AND outcome = 'cancelled')
      ),
      CHECK (orchestration_terminal_sequence > orchestration_started_sequence),
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_initial_planning_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (provider_delivery_id)
        REFERENCES agent_control_initial_planning_deliveries(provider_delivery_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (orchestration_started_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (orchestration_terminal_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (plan_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (stage_event_id, stage_run_id, stage_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (lease_event_id, lease_id, lease_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_finalization_receipts (
      finalization_command_id TEXT PRIMARY KEY CHECK (
        ${sql.literal(text("finalization_command_id"))}
      ),
      finalization_fingerprint TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(sha256("finalization_fingerprint"))}
      ),
      result_evidence_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("result_evidence_id"))}),
      handoff_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("handoff_id"))}),
      outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'cancelled')),
      stage_event_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("stage_event_id"))}),
      stage_event_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("stage_event_sequence"))}
      ),
      lease_event_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("lease_event_id"))}),
      lease_event_sequence INTEGER UNIQUE NOT NULL CHECK (
        ${sql.literal(positiveInteger("lease_event_sequence"))}
      ),
      accepted_at TEXT NOT NULL CHECK (${sql.literal(timestamp("accepted_at"))}),
      FOREIGN KEY (result_evidence_id)
        REFERENCES agent_control_initial_planning_result_evidence(result_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE TABLE agent_control_initial_planning_finalization_markers (
      marker_id TEXT PRIMARY KEY CHECK (${sql.literal(text("marker_id"))}),
      marker_fingerprint TEXT UNIQUE NOT NULL CHECK (${sql.literal(sha256("marker_fingerprint"))}),
      finalization_command_id TEXT UNIQUE NOT NULL CHECK (
        ${sql.literal(text("finalization_command_id"))}
      ),
      result_evidence_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("result_evidence_id"))}),
      handoff_id TEXT UNIQUE NOT NULL CHECK (${sql.literal(text("handoff_id"))}),
      committed_at TEXT NOT NULL CHECK (${sql.literal(timestamp("committed_at"))}),
      FOREIGN KEY (finalization_command_id)
        REFERENCES agent_control_initial_planning_finalization_receipts(finalization_command_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (result_evidence_id)
        REFERENCES agent_control_initial_planning_result_evidence(result_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;

  for (const table of [
    "agent_control_initial_planning_stage_started",
    "agent_control_initial_planning_result_evidence",
    "agent_control_initial_planning_finalization_receipts",
    "agent_control_initial_planning_finalization_markers",
  ] as const) {
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
       BEGIN SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable'); END`,
    ).unprepared;
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table}
       BEGIN SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable'); END`,
    ).unprepared;
  }

  const foreignKeyViolations = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
  if (foreignKeyViolations.length !== 0) {
    return yield* Effect.die(new Error("migration 052 introduced foreign-key violations"));
  }
  yield* sql`PRAGMA defer_foreign_keys = OFF`;
});
