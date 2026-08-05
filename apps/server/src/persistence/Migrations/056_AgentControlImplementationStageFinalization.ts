import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const quote = (identifier: string) => `"${identifier.replaceAll('"', '""')}"`;
const strictTextAllowEmpty = (column: string) => `
  typeof(${column}) = 'text'
  AND json_valid(json_array(${column})) = 1
  AND json_extract(json_array(${column}), '$[0]') IS ${column}
`;
const strictText = (column: string) =>
  `${strictTextAllowEmpty(column)} AND length(${column}) > 0 AND trim(${column}) = ${column}`;
const positive = (column: string) => `typeof(${column}) = 'integer' AND ${column} >= 1`;
const sha256 = (column: string) =>
  `${strictText(column)} AND length(${column}) = 64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;
const timestamp = (column: string) => `
  ${strictText(column)}
  AND length(${column}) = 24
  AND ${column} GLOB
    '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
  AND strftime('%Y-%m-%dT%H:%M:%fZ', ${column}) = ${column}
`;
const canonicalJson = (column: string) => `
  ${strictText(column)}
  AND json_valid(${column}) = 1
  AND json(${column}) = ${column}
`;

interface SchemaObject {
  readonly name: string;
  readonly sql: string;
}

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
    CREATE TABLE agent_control_events_rebuild_056 (
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
          'agentControl.stageRun.implementationCancelled'
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
  yield* sql`INSERT INTO agent_control_events_rebuild_056 SELECT * FROM agent_control_events`;
  yield* sql`DROP TABLE agent_control_events`;
  yield* sql`ALTER TABLE agent_control_events_rebuild_056 RENAME TO agent_control_events`;
  yield* restoreSchema(indexes);
  const sequence = sequenceRows[0]?.seq;
  if (sequence !== undefined) {
    yield* sql`
      DELETE FROM sqlite_sequence
      WHERE name IN ('agent_control_events', 'agent_control_events_rebuild_056')
    `;
    yield* sql`INSERT INTO sqlite_sequence(name, seq) VALUES ('agent_control_events', ${sequence})`;
  }
  yield* restoreSchema(triggers);
});

const createCompanions = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TABLE agent_control_implementation_result_evidence (
      result_evidence_id TEXT PRIMARY KEY,
      receipt_id TEXT UNIQUE NOT NULL,
      marker_id TEXT UNIQUE NOT NULL,
      finalization_command_id TEXT UNIQUE NOT NULL,
      finalization_fingerprint TEXT UNIQUE NOT NULL,
      result_json TEXT UNIQUE NOT NULL,
      outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'cancelled')),
      admission_evidence_id TEXT UNIQUE NOT NULL,
      admission_receipt_id TEXT UNIQUE NOT NULL,
      admission_marker_id TEXT UNIQUE NOT NULL,
      materialization_evidence_id TEXT UNIQUE NOT NULL,
      materialization_receipt_id TEXT UNIQUE NOT NULL,
      materialization_marker_id TEXT UNIQUE NOT NULL,
      start_evidence_id TEXT UNIQUE NOT NULL,
      start_receipt_id TEXT UNIQUE NOT NULL,
      start_marker_id TEXT UNIQUE NOT NULL,
      handoff_id TEXT UNIQUE NOT NULL,
      handoff_fingerprint TEXT UNIQUE NOT NULL,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL,
      task_source_event_id TEXT UNIQUE NOT NULL,
      task_source_event_sequence INTEGER UNIQUE NOT NULL CHECK (task_source_event_sequence >= 1),
      task_source_event_stream_version INTEGER NOT NULL CHECK (task_source_event_stream_version >= 1),
      repository_display TEXT NOT NULL,
      source_revision TEXT NOT NULL,
      task_title TEXT NOT NULL,
      task_body TEXT,
      stage_run_id TEXT UNIQUE NOT NULL,
      attempt_id TEXT UNIQUE NOT NULL,
      lease_id TEXT UNIQUE NOT NULL,
      lease_holder_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 1),
      worktree_reservation_id TEXT UNIQUE NOT NULL,
      worktree_revision INTEGER NOT NULL CHECK (worktree_revision >= 1),
      worktree_event_id TEXT UNIQUE NOT NULL,
      worktree_event_sequence INTEGER UNIQUE NOT NULL CHECK (worktree_event_sequence >= 1),
      worktree_event_stream_version INTEGER NOT NULL CHECK (worktree_event_stream_version >= 1),
      worktree_ownership_fingerprint TEXT NOT NULL,
      controlled_thread_reservation_id TEXT UNIQUE NOT NULL,
      thread_id TEXT UNIQUE NOT NULL,
      planning_thread_id TEXT NOT NULL,
      plan_id TEXT NOT NULL,
      proposed_plan_digest TEXT NOT NULL,
      turn_request_command_id TEXT UNIQUE NOT NULL,
      message_id TEXT UNIQUE NOT NULL,
      message_event_id TEXT UNIQUE NOT NULL,
      turn_request_event_id TEXT UNIQUE NOT NULL,
      provider_delivery_id TEXT UNIQUE NOT NULL,
      provider_instance_id TEXT NOT NULL,
      provider_turn_id TEXT NOT NULL,
      runtime_mode TEXT NOT NULL CHECK (runtime_mode IN ('approval-required', 'full-access')),
      model_selection_fingerprint TEXT NOT NULL,
      claim_generation INTEGER NOT NULL CHECK (claim_generation >= 1),
      attempt_count INTEGER NOT NULL CHECK (attempt_count >= 1),
      delivery_terminal_state TEXT NOT NULL CHECK (
        delivery_terminal_state IN ('completed', 'failed', 'interrupted')
      ),
      delivery_revision INTEGER NOT NULL CHECK (delivery_revision >= 1),
      terminal_at TEXT NOT NULL,
      orchestration_started_event_id TEXT UNIQUE NOT NULL,
      orchestration_started_sequence INTEGER UNIQUE NOT NULL CHECK (orchestration_started_sequence >= 1),
      orchestration_started_stream_version INTEGER NOT NULL CHECK (orchestration_started_stream_version >= 1),
      orchestration_terminal_event_id TEXT UNIQUE NOT NULL,
      orchestration_terminal_sequence INTEGER UNIQUE NOT NULL CHECK (orchestration_terminal_sequence >= 1),
      orchestration_terminal_stream_version INTEGER NOT NULL CHECK (orchestration_terminal_stream_version >= 1),
      orchestration_history_json TEXT UNIQUE NOT NULL,
      orchestration_history_digest TEXT UNIQUE NOT NULL,
      orchestration_history_event_count INTEGER NOT NULL CHECK (orchestration_history_event_count >= 1),
      stage_event_id TEXT UNIQUE NOT NULL,
      stage_event_sequence INTEGER UNIQUE NOT NULL CHECK (stage_event_sequence >= 1),
      stage_event_stream_version INTEGER NOT NULL CHECK (stage_event_stream_version = 3),
      lease_event_id TEXT UNIQUE NOT NULL,
      lease_event_sequence INTEGER UNIQUE NOT NULL CHECK (lease_event_sequence >= 1),
      lease_event_stream_version INTEGER NOT NULL CHECK (lease_event_stream_version >= 2),
      finalized_at TEXT NOT NULL,
      CHECK (
        (delivery_terminal_state = 'completed' AND outcome = 'succeeded')
        OR (delivery_terminal_state = 'failed' AND outcome = 'failed')
        OR (delivery_terminal_state = 'interrupted' AND outcome = 'cancelled')
      ),
      CHECK (orchestration_terminal_sequence > orchestration_started_sequence),
      FOREIGN KEY (admission_evidence_id)
        REFERENCES agent_control_implementation_admission_evidence(admission_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (materialization_evidence_id)
        REFERENCES agent_control_implementation_materialization_evidence(materialization_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (start_evidence_id)
        REFERENCES agent_control_implementation_stage_started_evidence(start_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_implementation_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (provider_delivery_id)
        REFERENCES agent_control_implementation_deliveries(provider_delivery_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (task_source_event_id)
        REFERENCES agent_control_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (worktree_event_id, worktree_reservation_id, worktree_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (message_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (turn_request_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (orchestration_started_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (orchestration_terminal_event_id)
        REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (stage_event_id, stage_run_id, stage_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (lease_event_id, lease_id, lease_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (receipt_id)
        REFERENCES agent_control_implementation_stage_finalization_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id)
        REFERENCES agent_control_implementation_stage_finalization_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TABLE agent_control_implementation_stage_finalization_receipts (
      receipt_id TEXT PRIMARY KEY,
      marker_id TEXT UNIQUE NOT NULL,
      finalization_command_id TEXT UNIQUE NOT NULL,
      finalization_fingerprint TEXT UNIQUE NOT NULL,
      result_evidence_id TEXT UNIQUE NOT NULL,
      handoff_id TEXT UNIQUE NOT NULL,
      outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'cancelled')),
      stage_event_id TEXT UNIQUE NOT NULL,
      stage_event_sequence INTEGER UNIQUE NOT NULL CHECK (stage_event_sequence >= 1),
      lease_event_id TEXT UNIQUE NOT NULL,
      lease_event_sequence INTEGER UNIQUE NOT NULL CHECK (lease_event_sequence >= 1),
      status TEXT NOT NULL CHECK (status = 'accepted'),
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (result_evidence_id)
        REFERENCES agent_control_implementation_result_evidence(result_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id)
        REFERENCES agent_control_implementation_stage_finalization_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TABLE agent_control_implementation_stage_finalization_markers (
      marker_id TEXT PRIMARY KEY,
      marker_fingerprint TEXT UNIQUE NOT NULL,
      receipt_id TEXT UNIQUE NOT NULL,
      finalization_command_id TEXT UNIQUE NOT NULL,
      finalization_fingerprint TEXT UNIQUE NOT NULL,
      result_evidence_id TEXT UNIQUE NOT NULL,
      handoff_id TEXT UNIQUE NOT NULL,
      committed_at TEXT NOT NULL,
      FOREIGN KEY (receipt_id)
        REFERENCES agent_control_implementation_stage_finalization_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (result_evidence_id)
        REFERENCES agent_control_implementation_result_evidence(result_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `).unprepared;
});

const createStorageTriggers = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_implementation_result_evidence_storage_validate
    BEFORE INSERT ON agent_control_implementation_result_evidence
    WHEN NOT COALESCE((
      ${[
        "result_evidence_id",
        "receipt_id",
        "marker_id",
        "finalization_command_id",
        "admission_evidence_id",
        "admission_receipt_id",
        "admission_marker_id",
        "materialization_evidence_id",
        "materialization_receipt_id",
        "materialization_marker_id",
        "start_evidence_id",
        "start_receipt_id",
        "start_marker_id",
        "handoff_id",
        "project_id",
        "task_id",
        "task_source_event_id",
        "repository_display",
        "source_revision",
        "task_title",
        "stage_run_id",
        "attempt_id",
        "lease_id",
        "lease_holder_id",
        "worktree_reservation_id",
        "worktree_event_id",
        "controlled_thread_reservation_id",
        "thread_id",
        "planning_thread_id",
        "plan_id",
        "turn_request_command_id",
        "message_id",
        "message_event_id",
        "turn_request_event_id",
        "provider_delivery_id",
        "provider_instance_id",
        "provider_turn_id",
        "orchestration_started_event_id",
        "orchestration_terminal_event_id",
        "stage_event_id",
        "lease_event_id",
      ]
        .map((column) => strictText(`NEW.${column}`))
        .join(" AND ")}
      AND ${[
        "finalization_fingerprint",
        "handoff_fingerprint",
        "source_identity_fingerprint",
        "worktree_ownership_fingerprint",
        "proposed_plan_digest",
        "model_selection_fingerprint",
        "orchestration_history_digest",
      ]
        .map((column) => sha256(`NEW.${column}`))
        .join(" AND ")}
      AND ${canonicalJson("NEW.result_json")}
      AND ${canonicalJson("NEW.orchestration_history_json")}
      AND (NEW.task_body IS NULL OR (${strictTextAllowEmpty("NEW.task_body")}))
      AND ${[
        "task_revision",
        "github_intake_sequence",
        "task_source_event_sequence",
        "task_source_event_stream_version",
        "fence_token",
        "worktree_revision",
        "worktree_event_sequence",
        "worktree_event_stream_version",
        "claim_generation",
        "attempt_count",
        "delivery_revision",
        "orchestration_started_sequence",
        "orchestration_started_stream_version",
        "orchestration_terminal_sequence",
        "orchestration_terminal_stream_version",
        "orchestration_history_event_count",
        "stage_event_sequence",
        "stage_event_stream_version",
        "lease_event_sequence",
        "lease_event_stream_version",
      ]
        .map((column) => positive(`NEW.${column}`))
        .join(" AND ")}
      AND ${timestamp("NEW.terminal_at")}
      AND ${timestamp("NEW.finalized_at")}
      AND ${strictText("NEW.outcome")}
      AND ${strictText("NEW.delivery_terminal_state")}
      AND ${strictText("NEW.runtime_mode")}
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid implementation result evidence storage'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_implementation_finalization_receipt_storage_validate
    BEFORE INSERT ON agent_control_implementation_stage_finalization_receipts
    WHEN NOT COALESCE((
      ${[
        "receipt_id",
        "marker_id",
        "finalization_command_id",
        "result_evidence_id",
        "handoff_id",
        "outcome",
        "stage_event_id",
        "lease_event_id",
        "status",
      ]
        .map((column) => strictText(`NEW.${column}`))
        .join(" AND ")}
      AND ${sha256("NEW.finalization_fingerprint")}
      AND ${positive("NEW.stage_event_sequence")}
      AND ${positive("NEW.lease_event_sequence")}
      AND ${timestamp("NEW.accepted_at")}
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid implementation finalization receipt storage'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_implementation_finalization_marker_storage_validate
    BEFORE INSERT ON agent_control_implementation_stage_finalization_markers
    WHEN NOT COALESCE((
      ${["marker_id", "receipt_id", "finalization_command_id", "result_evidence_id", "handoff_id"]
        .map((column) => strictText(`NEW.${column}`))
        .join(" AND ")}
      AND ${sha256("NEW.marker_fingerprint")}
      AND ${sha256("NEW.finalization_fingerprint")}
      AND ${timestamp("NEW.committed_at")}
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid implementation finalization marker storage'); END
  `).unprepared;
});

const createEventValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_implementation_terminal_stage_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.event_type IN (
      'agentControl.stageRun.implementationSucceeded',
      'agentControl.stageRun.implementationFailed',
      'agentControl.stageRun.implementationCancelled'
    ) AND NOT COALESCE((
      ${strictText("NEW.event_id")}
      AND NEW.aggregate_kind = 'stage-run'
      AND ${strictText("NEW.stream_id")}
      AND NEW.stream_version = 3
      AND ${timestamp("NEW.occurred_at")}
      AND ${strictText("NEW.command_id")}
      AND ${strictText("NEW.causation_event_id")}
      AND NEW.correlation_id = NEW.command_id
      AND NEW.actor_authority = 'system'
      AND ${canonicalJson("NEW.payload_json")}
      AND NEW.metadata_json = '{"schemaVersion":1}'
      AND json_type(NEW.payload_json) = 'object'
      AND json_extract(NEW.payload_json, '$.stageRunId') = NEW.stream_id
      AND json_extract(NEW.payload_json, '$.status') = CASE NEW.event_type
        WHEN 'agentControl.stageRun.implementationSucceeded' THEN 'succeeded'
        WHEN 'agentControl.stageRun.implementationFailed' THEN 'failed'
        ELSE 'cancelled' END
      AND json_extract(NEW.payload_json, '$.deliveryTerminalState') = CASE NEW.event_type
        WHEN 'agentControl.stageRun.implementationSucceeded' THEN 'completed'
        WHEN 'agentControl.stageRun.implementationFailed' THEN 'failed'
        ELSE 'interrupted' END
      AND json_extract(NEW.payload_json, '$.roleId') = 'implementer'
      AND json_extract(NEW.payload_json, '$.stageKind') = 'implementation'
      AND json_extract(NEW.payload_json, '$.stageOrdinal') = 2
      AND json_extract(NEW.payload_json, '$.attemptOrdinal') = 1
      AND json_extract(NEW.payload_json, '$.finalizedAt') = NEW.occurred_at
      AND json_extract(NEW.payload_json, '$.providerTerminalEventId') = NEW.causation_event_id
      AND EXISTS (
        SELECT 1
        FROM agent_control_implementation_stage_started_evidence started
        JOIN agent_control_implementation_stage_started_receipts receipt
          ON receipt.start_evidence_id = started.start_evidence_id
        JOIN agent_control_implementation_stage_started_markers marker
          ON marker.start_evidence_id = started.start_evidence_id
        JOIN agent_control_implementation_handoff_intents intent
          ON intent.handoff_id = started.handoff_id
        JOIN agent_control_implementation_materialization_evidence materialization
          ON materialization.materialization_evidence_id = started.materialization_evidence_id
        JOIN agent_control_implementation_deliveries delivery
          ON delivery.provider_delivery_id = started.provider_delivery_id
        JOIN agent_control_stage_run_states stage ON stage.stage_run_id = started.stage_run_id
        JOIN agent_control_events predecessor ON predecessor.event_id = started.stage_event_id
        JOIN orchestration_events terminal ON terminal.event_id = NEW.causation_event_id
        WHERE started.stage_run_id = NEW.stream_id
          AND started.stage_event_stream_version = 2
          AND predecessor.stream_version = 2
          AND predecessor.event_type = 'agentControl.stageRun.implementationStarted'
          AND stage.status = 'running' AND stage.revision = 2
          AND delivery.state = json_extract(NEW.payload_json, '$.deliveryTerminalState')
          AND delivery.terminal_at = NEW.occurred_at
          AND delivery.provider_turn_id = started.provider_turn_id
          AND delivery.revision = json_extract(NEW.payload_json, '$.deliveryRevision')
          AND terminal.stream_id = started.thread_id
          AND terminal.event_type = 'thread.session-set'
          AND terminal.actor_kind = 'provider'
          AND terminal.occurred_at = NEW.occurred_at
          AND started.start_evidence_id = json_extract(NEW.payload_json, '$.startEvidenceId')
          AND receipt.start_receipt_id = json_extract(NEW.payload_json, '$.startReceiptId')
          AND marker.start_marker_id = json_extract(NEW.payload_json, '$.startMarkerId')
          AND started.admission_evidence_id = json_extract(NEW.payload_json, '$.admissionEvidenceId')
          AND started.admission_receipt_id = json_extract(NEW.payload_json, '$.admissionReceiptId')
          AND started.admission_marker_id = json_extract(NEW.payload_json, '$.admissionMarkerId')
          AND started.materialization_evidence_id = json_extract(NEW.payload_json, '$.materializationEvidenceId')
          AND started.materialization_receipt_id = json_extract(NEW.payload_json, '$.materializationReceiptId')
          AND started.materialization_marker_id = json_extract(NEW.payload_json, '$.materializationMarkerId')
          AND materialization.repository_display = json_extract(NEW.payload_json, '$.repositoryDisplay')
          AND materialization.source_revision = json_extract(NEW.payload_json, '$.sourceRevision')
          AND started.handoff_id = json_extract(NEW.payload_json, '$.handoffId')
          AND started.handoff_fingerprint = json_extract(NEW.payload_json, '$.handoffFingerprint')
          AND started.provider_delivery_id = json_extract(NEW.payload_json, '$.providerDeliveryId')
          AND started.claim_generation = json_extract(NEW.payload_json, '$.claimGeneration')
          AND started.attempt_count = json_extract(NEW.payload_json, '$.attemptCount')
          AND started.project_id = json_extract(NEW.payload_json, '$.projectId')
          AND started.task_id = json_extract(NEW.payload_json, '$.taskId')
          AND started.task_revision = json_extract(NEW.payload_json, '$.taskRevision')
          AND started.github_intake_sequence = json_extract(NEW.payload_json, '$.githubIntakeSequence')
          AND started.source_identity_fingerprint = json_extract(NEW.payload_json, '$.sourceIdentityFingerprint')
          AND started.attempt_id = json_extract(NEW.payload_json, '$.attemptId')
          AND started.controlled_thread_reservation_id = json_extract(NEW.payload_json, '$.controlledThreadReservationId')
          AND started.thread_id = json_extract(NEW.payload_json, '$.threadId')
          AND started.planning_thread_id = json_extract(NEW.payload_json, '$.planningThreadId')
          AND started.plan_id = json_extract(NEW.payload_json, '$.planId')
          AND started.proposed_plan_digest = json_extract(NEW.payload_json, '$.proposedPlanDigest')
          AND started.provider_instance_id = json_extract(NEW.payload_json, '$.providerInstanceId')
          AND started.provider_turn_id = json_extract(NEW.payload_json, '$.providerTurnId')
          AND started.runtime_mode = json_extract(NEW.payload_json, '$.runtimeMode')
          AND started.model_selection_fingerprint = json_extract(NEW.payload_json, '$.modelSelectionFingerprint')
          AND started.lease_id = json_extract(NEW.payload_json, '$.leaseId')
          AND started.lease_holder_id = json_extract(NEW.payload_json, '$.leaseHolderId')
          AND started.fence_token = json_extract(NEW.payload_json, '$.fenceToken')
          AND intent.task_source_event_id = json_extract(NEW.payload_json, '$.taskSourceEventId')
          AND intent.task_source_event_sequence = json_extract(NEW.payload_json, '$.taskSourceEventSequence')
          AND intent.task_source_event_stream_version = json_extract(NEW.payload_json, '$.taskSourceEventStreamVersion')
          AND intent.worktree_reservation_id = json_extract(NEW.payload_json, '$.worktreeReservationId')
          AND intent.worktree_event_id = json_extract(NEW.payload_json, '$.worktreeEventId')
          AND intent.worktree_event_sequence = json_extract(NEW.payload_json, '$.worktreeEventSequence')
          AND intent.worktree_event_stream_version = json_extract(NEW.payload_json, '$.worktreeEventStreamVersion')
          AND intent.worktree_ownership_fingerprint = json_extract(NEW.payload_json, '$.worktreeOwnershipFingerprint')
          AND intent.turn_request_command_id = json_extract(NEW.payload_json, '$.turnRequestCommandId')
          AND intent.message_id = json_extract(NEW.payload_json, '$.messageId')
          AND intent.message_event_id = json_extract(NEW.payload_json, '$.messageEventId')
          AND intent.turn_request_event_id = json_extract(NEW.payload_json, '$.turnRequestEventId')
      )
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid implementation terminal stage event'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_implementation_lease_release_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.event_type = 'agentControl.stageRunLease.releasedAfterImplementation'
      AND NOT COALESCE((
        ${strictText("NEW.event_id")}
        AND NEW.aggregate_kind = 'stage-run-lease'
        AND ${strictText("NEW.stream_id")}
        AND ${timestamp("NEW.occurred_at")}
        AND ${strictText("NEW.command_id")}
        AND ${strictText("NEW.causation_event_id")}
        AND NEW.correlation_id = NEW.command_id
        AND NEW.actor_authority = 'system'
        AND ${canonicalJson("NEW.payload_json")}
        AND NEW.metadata_json = '{"schemaVersion":1}'
        AND json_extract(NEW.payload_json, '$.leaseId') = NEW.stream_id
        AND json_extract(NEW.payload_json, '$.releasedAt') = NEW.occurred_at
        AND json_extract(NEW.payload_json, '$.stageEventId') = NEW.causation_event_id
        AND EXISTS (
          SELECT 1
          FROM agent_control_stage_run_lease_states lease
          JOIN agent_control_implementation_stage_started_evidence started
            ON started.lease_id = lease.lease_id
          JOIN agent_control_implementation_stage_started_receipts receipt
            ON receipt.start_evidence_id = started.start_evidence_id
          JOIN agent_control_implementation_stage_started_markers marker
            ON marker.start_evidence_id = started.start_evidence_id
          JOIN agent_control_implementation_deliveries delivery
            ON delivery.provider_delivery_id = started.provider_delivery_id
          JOIN agent_control_events terminal_stage ON terminal_stage.event_id = NEW.causation_event_id
          WHERE lease.lease_id = NEW.stream_id
            AND lease.status = 'reserved'
            AND NEW.stream_version = lease.revision + 1
            AND lease.stage_run_id = json_extract(NEW.payload_json, '$.stageRunId')
            AND lease.attempt_id = json_extract(NEW.payload_json, '$.attemptId')
            AND lease.holder_id = json_extract(NEW.payload_json, '$.holderId')
            AND lease.fence_token = json_extract(NEW.payload_json, '$.fenceToken')
            AND started.start_evidence_id = json_extract(NEW.payload_json, '$.startEvidenceId')
            AND receipt.start_receipt_id = json_extract(NEW.payload_json, '$.startReceiptId')
            AND marker.start_marker_id = json_extract(NEW.payload_json, '$.startMarkerId')
            AND started.admission_evidence_id = json_extract(NEW.payload_json, '$.admissionEvidenceId')
            AND started.admission_receipt_id = json_extract(NEW.payload_json, '$.admissionReceiptId')
            AND started.admission_marker_id = json_extract(NEW.payload_json, '$.admissionMarkerId')
            AND started.materialization_evidence_id = json_extract(NEW.payload_json, '$.materializationEvidenceId')
            AND started.materialization_receipt_id = json_extract(NEW.payload_json, '$.materializationReceiptId')
            AND started.materialization_marker_id = json_extract(NEW.payload_json, '$.materializationMarkerId')
            AND started.handoff_id = json_extract(NEW.payload_json, '$.handoffId')
            AND started.handoff_fingerprint = json_extract(NEW.payload_json, '$.handoffFingerprint')
            AND started.provider_delivery_id = json_extract(NEW.payload_json, '$.providerDeliveryId')
            AND delivery.state = json_extract(NEW.payload_json, '$.deliveryTerminalState')
            AND delivery.revision = json_extract(NEW.payload_json, '$.deliveryRevision')
            AND started.project_id = json_extract(NEW.payload_json, '$.projectId')
            AND started.task_id = json_extract(NEW.payload_json, '$.taskId')
            AND started.task_revision = json_extract(NEW.payload_json, '$.taskRevision')
            AND started.github_intake_sequence = json_extract(NEW.payload_json, '$.githubIntakeSequence')
            AND started.source_identity_fingerprint = json_extract(NEW.payload_json, '$.sourceIdentityFingerprint')
            AND started.controlled_thread_reservation_id = json_extract(NEW.payload_json, '$.controlledThreadReservationId')
            AND started.thread_id = json_extract(NEW.payload_json, '$.threadId')
            AND started.planning_thread_id = json_extract(NEW.payload_json, '$.planningThreadId')
            AND started.plan_id = json_extract(NEW.payload_json, '$.planId')
            AND started.proposed_plan_digest = json_extract(NEW.payload_json, '$.proposedPlanDigest')
            AND started.provider_instance_id = json_extract(NEW.payload_json, '$.providerInstanceId')
            AND started.provider_turn_id = json_extract(NEW.payload_json, '$.providerTurnId')
            AND started.runtime_mode = json_extract(NEW.payload_json, '$.runtimeMode')
            AND started.model_selection_fingerprint = json_extract(NEW.payload_json, '$.modelSelectionFingerprint')
            AND terminal_stage.stream_id = started.stage_run_id
            AND terminal_stage.stream_version = 3
            AND terminal_stage.event_type = CASE json_extract(NEW.payload_json, '$.stageStatus')
              WHEN 'succeeded' THEN 'agentControl.stageRun.implementationSucceeded'
              WHEN 'failed' THEN 'agentControl.stageRun.implementationFailed'
              WHEN 'cancelled' THEN 'agentControl.stageRun.implementationCancelled' END
            AND json_extract(terminal_stage.payload_json, '$.resultEvidenceId') =
              json_extract(NEW.payload_json, '$.resultEvidenceId')
            AND json_extract(terminal_stage.payload_json, '$.orchestrationHistoryDigest') =
              json_extract(NEW.payload_json, '$.orchestrationHistoryDigest')
        )
      ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid implementation lease release event'); END
  `).unprepared;

  for (const operation of ["UPDATE", "DELETE"] as const) {
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_implementation_finalization_event_no_${operation.toLowerCase()}
      BEFORE ${operation} ON agent_control_events
      WHEN OLD.event_type IN (
        'agentControl.stageRun.implementationSucceeded',
        'agentControl.stageRun.implementationFailed',
        'agentControl.stageRun.implementationCancelled',
        'agentControl.stageRunLease.releasedAfterImplementation'
      )
      BEGIN SELECT RAISE(ABORT, 'implementation finalization events are immutable'); END
    `).unprepared;
  }
});

const createCompanionValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_implementation_result_evidence_validate
    BEFORE INSERT ON agent_control_implementation_result_evidence
    WHEN NOT COALESCE((
      json_extract(NEW.result_json, '$.schemaVersion') = 1
      AND json_extract(NEW.result_json, '$.resultEvidenceId') = NEW.result_evidence_id
      AND json_extract(NEW.result_json, '$.finalizationCommandId') = NEW.finalization_command_id
      AND json_extract(NEW.result_json, '$.handoffId') = NEW.handoff_id
      AND json_extract(NEW.result_json, '$.handoffFingerprint') = NEW.handoff_fingerprint
      AND json_extract(NEW.result_json, '$.outcome') = NEW.outcome
      AND json_extract(NEW.result_json, '$.stageEventId') = NEW.stage_event_id
      AND json_extract(NEW.result_json, '$.stageEventSequence') = NEW.stage_event_sequence
      AND json_extract(NEW.result_json, '$.leaseEventId') = NEW.lease_event_id
      AND json_extract(NEW.result_json, '$.leaseEventSequence') = NEW.lease_event_sequence
      AND json_extract(NEW.result_json, '$.orchestrationHistoryDigest') = NEW.orchestration_history_digest
      AND json_extract(NEW.result_json, '$.orchestrationHistory') = NEW.orchestration_history_json
      AND json_array_length(NEW.orchestration_history_json) = NEW.orchestration_history_event_count
      AND json_extract(NEW.result_json, '$.finalizedAt') = NEW.finalized_at
      AND EXISTS (
        SELECT 1
        FROM agent_control_implementation_handoff_intents intent
        JOIN agent_control_implementation_handoff_receipts handoff_receipt
          ON handoff_receipt.handoff_id = intent.handoff_id
        JOIN agent_control_implementation_handoff_accepted accepted
          ON accepted.handoff_id = intent.handoff_id
        JOIN agent_control_implementation_materialization_evidence materialization
          ON materialization.materialization_evidence_id = intent.materialization_evidence_id
        JOIN agent_control_implementation_materialization_receipts materialization_receipt
          ON materialization_receipt.materialization_evidence_id = materialization.materialization_evidence_id
        JOIN agent_control_implementation_materialization_markers materialization_marker
          ON materialization_marker.materialization_evidence_id = materialization.materialization_evidence_id
        JOIN agent_control_implementation_admission_evidence admission
          ON admission.admission_evidence_id = materialization.admission_evidence_id
        JOIN agent_control_implementation_admission_receipts admission_receipt
          ON admission_receipt.admission_evidence_id = admission.admission_evidence_id
        JOIN agent_control_implementation_admission_markers admission_marker
          ON admission_marker.admission_evidence_id = admission.admission_evidence_id
        JOIN agent_control_implementation_deliveries delivery ON delivery.handoff_id = intent.handoff_id
        JOIN agent_control_implementation_stage_started_evidence started
          ON started.provider_delivery_id = delivery.provider_delivery_id
        JOIN agent_control_implementation_stage_started_receipts start_receipt
          ON start_receipt.start_evidence_id = started.start_evidence_id
        JOIN agent_control_implementation_stage_started_markers start_marker
          ON start_marker.start_evidence_id = started.start_evidence_id
        JOIN orchestration_events orchestration_started
          ON orchestration_started.event_id = NEW.orchestration_started_event_id
        JOIN orchestration_events orchestration_terminal
          ON orchestration_terminal.event_id = NEW.orchestration_terminal_event_id
        JOIN agent_control_events stage_event ON stage_event.event_id = NEW.stage_event_id
        JOIN agent_control_events lease_event ON lease_event.event_id = NEW.lease_event_id
        JOIN agent_control_stage_run_states stage ON stage.stage_run_id = NEW.stage_run_id
        JOIN agent_control_stage_run_lease_states lease ON lease.lease_id = NEW.lease_id
        WHERE intent.handoff_id = NEW.handoff_id
          AND intent.handoff_fingerprint = NEW.handoff_fingerprint
          AND handoff_receipt.status = 'accepted'
          AND accepted.provider_delivery_id = NEW.provider_delivery_id
          AND admission.admission_evidence_id = NEW.admission_evidence_id
          AND admission_receipt.receipt_id = NEW.admission_receipt_id
          AND admission_marker.marker_id = NEW.admission_marker_id
          AND materialization.materialization_evidence_id = NEW.materialization_evidence_id
          AND materialization_receipt.materialization_receipt_id = NEW.materialization_receipt_id
          AND materialization_marker.materialization_marker_id = NEW.materialization_marker_id
          AND materialization.repository_display = NEW.repository_display
          AND materialization.source_revision = NEW.source_revision
          AND materialization.task_title = NEW.task_title
          AND materialization.task_body IS NEW.task_body
          AND started.start_evidence_id = NEW.start_evidence_id
          AND start_receipt.start_receipt_id = NEW.start_receipt_id
          AND start_marker.start_marker_id = NEW.start_marker_id
          AND intent.project_id = NEW.project_id
          AND intent.task_id = NEW.task_id
          AND intent.task_revision = NEW.task_revision
          AND intent.github_intake_sequence = NEW.github_intake_sequence
          AND intent.source_identity_fingerprint = NEW.source_identity_fingerprint
          AND intent.task_source_event_id = NEW.task_source_event_id
          AND intent.task_source_event_sequence = NEW.task_source_event_sequence
          AND intent.task_source_event_stream_version = NEW.task_source_event_stream_version
          AND intent.stage_run_id = NEW.stage_run_id
          AND intent.attempt_id = NEW.attempt_id
          AND intent.lease_id = NEW.lease_id
          AND intent.lease_holder_id = NEW.lease_holder_id
          AND intent.fence_token = NEW.fence_token
          AND intent.worktree_reservation_id = NEW.worktree_reservation_id
          AND intent.worktree_revision = NEW.worktree_revision
          AND intent.worktree_event_id = NEW.worktree_event_id
          AND intent.worktree_event_sequence = NEW.worktree_event_sequence
          AND intent.worktree_event_stream_version = NEW.worktree_event_stream_version
          AND intent.worktree_ownership_fingerprint = NEW.worktree_ownership_fingerprint
          AND intent.controlled_thread_reservation_id = NEW.controlled_thread_reservation_id
          AND intent.thread_id = NEW.thread_id
          AND intent.planning_thread_id = NEW.planning_thread_id
          AND intent.plan_id = NEW.plan_id
          AND intent.proposed_plan_digest = NEW.proposed_plan_digest
          AND intent.turn_request_command_id = NEW.turn_request_command_id
          AND intent.message_id = NEW.message_id
          AND intent.message_event_id = NEW.message_event_id
          AND intent.turn_request_event_id = NEW.turn_request_event_id
          AND intent.provider_instance_id = NEW.provider_instance_id
          AND intent.runtime_mode = NEW.runtime_mode
          AND intent.model_selection_fingerprint = NEW.model_selection_fingerprint
          AND delivery.provider_turn_id = NEW.provider_turn_id
          AND delivery.state = NEW.delivery_terminal_state
          AND delivery.revision = NEW.delivery_revision
          AND delivery.claim_generation = NEW.claim_generation
          AND delivery.attempt_count = NEW.attempt_count
          AND delivery.terminal_at = NEW.terminal_at
          AND delivery.terminal_at = NEW.finalized_at
          AND started.provider_turn_id = NEW.provider_turn_id
          AND orchestration_started.sequence = NEW.orchestration_started_sequence
          AND orchestration_started.stream_version = NEW.orchestration_started_stream_version
          AND orchestration_started.event_type = 'thread.session-set'
          AND orchestration_started.actor_kind = 'provider'
          AND orchestration_started.stream_id = NEW.thread_id
          AND orchestration_started.occurred_at = started.started_at
          AND json_extract(orchestration_started.payload_json, '$.session.status') = 'running'
          AND json_extract(orchestration_started.payload_json, '$.session.activeTurnId') = NEW.provider_turn_id
          AND json_extract(orchestration_started.payload_json, '$.session.providerInstanceId') = NEW.provider_instance_id
          AND json_extract(orchestration_started.payload_json, '$.session.runtimeMode') = NEW.runtime_mode
          AND orchestration_terminal.sequence = NEW.orchestration_terminal_sequence
          AND orchestration_terminal.stream_version = NEW.orchestration_terminal_stream_version
          AND orchestration_terminal.event_type = 'thread.session-set'
          AND orchestration_terminal.actor_kind = 'provider'
          AND orchestration_terminal.stream_id = NEW.thread_id
          AND orchestration_terminal.occurred_at = NEW.terminal_at
          AND json_extract(orchestration_terminal.payload_json, '$.session.activeTurnId') IS NULL
          AND json_extract(orchestration_terminal.payload_json, '$.session.providerInstanceId') = NEW.provider_instance_id
          AND json_extract(orchestration_terminal.payload_json, '$.session.runtimeMode') = NEW.runtime_mode
          AND json_extract(orchestration_terminal.payload_json, '$.session.status') = CASE NEW.outcome
            WHEN 'failed' THEN 'error' ELSE 'ready' END
          AND stage_event.stream_id = NEW.stage_run_id
          AND stage_event.stream_version = 3
          AND stage_event.sequence = NEW.stage_event_sequence
          AND stage_event.event_type = CASE NEW.outcome
            WHEN 'succeeded' THEN 'agentControl.stageRun.implementationSucceeded'
            WHEN 'failed' THEN 'agentControl.stageRun.implementationFailed'
            WHEN 'cancelled' THEN 'agentControl.stageRun.implementationCancelled' END
          AND json_extract(stage_event.payload_json, '$.resultEvidenceId') = NEW.result_evidence_id
          AND json_extract(stage_event.payload_json, '$.orchestrationHistoryDigest') = NEW.orchestration_history_digest
          AND lease_event.stream_id = NEW.lease_id
          AND lease_event.stream_version = NEW.lease_event_stream_version
          AND lease_event.sequence = NEW.lease_event_sequence
          AND lease_event.event_type = 'agentControl.stageRunLease.releasedAfterImplementation'
          AND json_extract(lease_event.payload_json, '$.resultEvidenceId') = NEW.result_evidence_id
          AND json_extract(lease_event.payload_json, '$.stageEventId') = NEW.stage_event_id
          AND json_extract(lease_event.payload_json, '$.orchestrationHistoryDigest') = NEW.orchestration_history_digest
          AND stage.status = NEW.outcome AND stage.revision = 3
          AND stage.last_event_sequence = NEW.stage_event_sequence
          AND lease.status = 'released'
          AND lease.revision = NEW.lease_event_stream_version
          AND lease.last_event_sequence = NEW.lease_event_sequence
          AND lease.holder_id = NEW.lease_holder_id
          AND lease.fence_token = NEW.fence_token
      )
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'implementation result evidence is inconsistent'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_implementation_finalization_receipt_validate
    BEFORE INSERT ON agent_control_implementation_stage_finalization_receipts
    WHEN NOT COALESCE((EXISTS (
      SELECT 1 FROM agent_control_implementation_result_evidence evidence
      WHERE evidence.result_evidence_id = NEW.result_evidence_id
        AND evidence.receipt_id = NEW.receipt_id
        AND evidence.marker_id = NEW.marker_id
        AND evidence.finalization_command_id = NEW.finalization_command_id
        AND evidence.finalization_fingerprint = NEW.finalization_fingerprint
        AND evidence.handoff_id = NEW.handoff_id
        AND evidence.outcome = NEW.outcome
        AND evidence.stage_event_id = NEW.stage_event_id
        AND evidence.stage_event_sequence = NEW.stage_event_sequence
        AND evidence.lease_event_id = NEW.lease_event_id
        AND evidence.lease_event_sequence = NEW.lease_event_sequence
        AND evidence.finalized_at = NEW.accepted_at
        AND NEW.status = 'accepted'
    )), 0)
    BEGIN SELECT RAISE(ABORT, 'implementation finalization receipt is inconsistent'); END
  `).unprepared;

  yield* sql.unsafe(`
    CREATE TRIGGER agent_control_implementation_finalization_marker_validate
    BEFORE INSERT ON agent_control_implementation_stage_finalization_markers
    WHEN NOT COALESCE((EXISTS (
      SELECT 1
      FROM agent_control_implementation_result_evidence evidence
      JOIN agent_control_implementation_stage_finalization_receipts receipt
        ON receipt.result_evidence_id = evidence.result_evidence_id
      WHERE evidence.marker_id = NEW.marker_id
        AND evidence.receipt_id = NEW.receipt_id
        AND receipt.receipt_id = NEW.receipt_id
        AND evidence.finalization_command_id = NEW.finalization_command_id
        AND receipt.finalization_command_id = NEW.finalization_command_id
        AND evidence.finalization_fingerprint = NEW.finalization_fingerprint
        AND receipt.finalization_fingerprint = NEW.finalization_fingerprint
        AND evidence.result_evidence_id = NEW.result_evidence_id
        AND evidence.handoff_id = NEW.handoff_id
        AND receipt.handoff_id = NEW.handoff_id
        AND evidence.finalized_at = NEW.committed_at
        AND receipt.accepted_at = NEW.committed_at
        AND receipt.status = 'accepted'
    )), 0)
    BEGIN SELECT RAISE(ABORT, 'implementation finalization marker is inconsistent'); END
  `).unprepared;

  for (const table of [
    "agent_control_implementation_result_evidence",
    "agent_control_implementation_stage_finalization_receipts",
    "agent_control_implementation_stage_finalization_markers",
  ] as const) {
    yield* sql.unsafe(`
      CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
      BEGIN SELECT RAISE(ABORT, 'implementation finalization evidence is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table}
      BEGIN SELECT RAISE(ABORT, 'implementation finalization evidence is immutable'); END
    `).unprepared;
  }
});

/** Durable terminal Implementation Stage evidence and exact-once lease release. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const existing = yield* sql<{ readonly count: number }>`
    SELECT count(*) AS count FROM sqlite_schema
    WHERE type = 'table'
      AND name = 'agent_control_implementation_stage_finalization_markers'
  `;
  if (existing[0]?.count === 1) return;

  yield* sql`PRAGMA defer_foreign_keys = ON`;
  yield* rebuildAgentControlEvents;
  yield* createCompanions;
  yield* createStorageTriggers;
  yield* createEventValidation;
  yield* createCompanionValidation;

  const violations = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
  if (violations.length !== 0) {
    return yield* Effect.die(new Error("migration 056 introduced foreign-key violations"));
  }
  yield* sql`PRAGMA defer_foreign_keys = OFF`;
});
