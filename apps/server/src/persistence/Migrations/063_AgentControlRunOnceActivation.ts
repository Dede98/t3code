import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { sha256Utf8 } from "../../agentControl/initialPlanning/eventEvidence.ts";

const CANONICAL_BLOB_MATCH = "t3_run_once_canonical_blob_match";
const ACTIVATION_IDENTITY_MATCH = "t3_run_once_activation_identity_match";
const STEP_IDENTITY_MATCH = "t3_run_once_step_identity_match";
const MARKER_MATCH = "t3_run_once_marker_match";
const SOURCE_FINGERPRINT_MATCH = "t3_run_once_source_fingerprint_match";

interface SchemaObject {
  readonly name: string;
  readonly sql: string;
}

const quote = (identifier: string) => `"${identifier.replaceAll('"', '""')}"`;
const restoreSchema = (objects: ReadonlyArray<SchemaObject>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const object of objects) yield* sql.unsafe(object.sql).unprepared;
  });

export type Migration063FaultPoint =
  | "before-project-state-rebuild"
  | "after-project-state-rebuild"
  | "after-run-once-tables"
  | "after-run-once-triggers";

const rebuildProjectStates = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [triggers, indexes] = yield* Effect.all([
    sql<SchemaObject>`
      SELECT name, sql FROM main.sqlite_schema
      WHERE type = 'trigger' AND tbl_name = 'agent_control_project_states' AND sql IS NOT NULL
      ORDER BY name
    `,
    sql<SchemaObject>`
      SELECT name, sql FROM main.sqlite_schema
      WHERE type = 'index' AND tbl_name = 'agent_control_project_states' AND sql IS NOT NULL
      ORDER BY name
    `,
  ]);
  for (const trigger of triggers) {
    yield* sql.unsafe(`DROP TRIGGER main.${quote(trigger.name)}`).unprepared;
  }
  yield* sql`
    CREATE TABLE main.agent_control_project_states_rebuild_063 (
      project_id TEXT PRIMARY KEY,
      mode TEXT NOT NULL CHECK (
        typeof(mode) = 'text' AND mode IN ('manual', 'observe', 'run-once', 'paused')
      ),
      paused_from_mode TEXT CHECK (
        paused_from_mode IS NULL OR (
          typeof(paused_from_mode) = 'text'
          AND paused_from_mode IN ('observe', 'run-once')
        )
      ),
      revision INTEGER NOT NULL CHECK (typeof(revision) = 'integer' AND revision >= 1),
      last_event_sequence INTEGER NOT NULL CHECK (
        typeof(last_event_sequence) = 'integer' AND last_event_sequence >= 1
      ),
      updated_at TEXT NOT NULL CHECK (typeof(updated_at) = 'text' AND length(updated_at) >= 20),
      CHECK (
        (mode = 'paused' AND paused_from_mode IN ('observe', 'run-once'))
        OR (mode != 'paused' AND paused_from_mode IS NULL)
      )
    )
  `;
  yield* sql`
    INSERT INTO main.agent_control_project_states_rebuild_063
    SELECT * FROM main.agent_control_project_states
  `;
  yield* sql`DROP TABLE main.agent_control_project_states`;
  yield* sql`
    ALTER TABLE main.agent_control_project_states_rebuild_063
    RENAME TO agent_control_project_states
  `;
  yield* restoreSchema(indexes);
  yield* restoreSchema(triggers);
});

const createTables = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE main.agent_control_run_once_activations (
      run_id TEXT PRIMARY KEY CHECK (typeof(run_id) = 'text' AND length(run_id) > 64),
      project_id TEXT NOT NULL CHECK (typeof(project_id) = 'text' AND length(project_id) > 0),
      activation_event_id TEXT NOT NULL UNIQUE CHECK (
        typeof(activation_event_id) = 'text' AND length(activation_event_id) > 0
      ),
      activation_event_sequence INTEGER NOT NULL CHECK (
        typeof(activation_event_sequence) = 'integer' AND activation_event_sequence >= 1
      ),
      activation_event_stream_version INTEGER NOT NULL CHECK (
        typeof(activation_event_stream_version) = 'integer'
        AND activation_event_stream_version >= 1
      ),
      activation_command_id TEXT NOT NULL UNIQUE CHECK (
        typeof(activation_command_id) = 'text' AND length(activation_command_id) > 0
      ),
      github_intake_sequence INTEGER NOT NULL CHECK (
        typeof(github_intake_sequence) = 'integer' AND github_intake_sequence >= 1
      ),
      github_event_id TEXT NOT NULL CHECK (
        typeof(github_event_id) = 'text' AND length(github_event_id) > 0
      ),
      github_event_sequence INTEGER NOT NULL CHECK (
        typeof(github_event_sequence) = 'integer' AND github_event_sequence >= 1
      ),
      github_event_stream_version INTEGER NOT NULL CHECK (
        typeof(github_event_stream_version) = 'integer' AND github_event_stream_version >= 1
      ),
      reconcile_revision INTEGER NOT NULL CHECK (
        typeof(reconcile_revision) = 'integer' AND reconcile_revision >= 1
      ),
      source_fingerprint TEXT NOT NULL CHECK (
        typeof(source_fingerprint) = 'text' AND length(source_fingerprint) = 64
        AND source_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      activated_at TEXT NOT NULL CHECK (
        typeof(activated_at) = 'text'
        AND COALESCE(activated_at = strftime('%Y-%m-%dT%H:%M:%fZ', activated_at), 0)
      ),
      UNIQUE (run_id, project_id),
      UNIQUE (project_id, activation_event_id),
      FOREIGN KEY (activation_event_id) REFERENCES agent_control_events(event_id),
      FOREIGN KEY (github_event_id) REFERENCES agent_control_events(event_id)
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_run_once_step_evidence (
      evidence_id TEXT PRIMARY KEY CHECK (typeof(evidence_id) = 'text' AND length(evidence_id) > 64),
      receipt_id TEXT NOT NULL UNIQUE CHECK (typeof(receipt_id) = 'text' AND length(receipt_id) > 64),
      marker_id TEXT NOT NULL UNIQUE CHECK (typeof(marker_id) = 'text' AND length(marker_id) > 64),
      run_id TEXT NOT NULL,
      project_id TEXT NOT NULL CHECK (typeof(project_id) = 'text' AND length(project_id) > 0),
      ordinal INTEGER NOT NULL CHECK (typeof(ordinal) = 'integer' AND ordinal >= 1),
      step TEXT NOT NULL CHECK (typeof(step) = 'text' AND step IN (
        'activation-admitted', 'task-selected', 'no-eligible-task', 'stage-prepared',
        'lease-reserved', 'worktree-ready', 'thread-activated', 'task-terminal-observed',
        'mode-reset', 'mode-reset-superseded', 'completed'
      )),
      command_id TEXT NOT NULL UNIQUE CHECK (typeof(command_id) = 'text' AND length(command_id) > 0),
      payload_json BLOB NOT NULL CHECK (typeof(payload_json) = 'blob'),
      payload_fingerprint TEXT NOT NULL CHECK (
        typeof(payload_fingerprint) = 'text' AND length(payload_fingerprint) = 64
        AND payload_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      task_id TEXT CHECK (task_id IS NULL OR (typeof(task_id) = 'text' AND length(task_id) > 0)),
      stage_run_id TEXT CHECK (
        stage_run_id IS NULL OR (typeof(stage_run_id) = 'text' AND length(stage_run_id) > 0)
      ),
      lease_id TEXT CHECK (lease_id IS NULL OR (typeof(lease_id) = 'text' AND length(lease_id) > 0)),
      worktree_reservation_id TEXT CHECK (
        worktree_reservation_id IS NULL
        OR (typeof(worktree_reservation_id) = 'text' AND length(worktree_reservation_id) > 0)
      ),
      controlled_thread_reservation_id TEXT CHECK (
        controlled_thread_reservation_id IS NULL
        OR (
          typeof(controlled_thread_reservation_id) = 'text'
          AND length(controlled_thread_reservation_id) > 0
        )
      ),
      terminal_task_event_id TEXT CHECK (
        terminal_task_event_id IS NULL
        OR (typeof(terminal_task_event_id) = 'text' AND length(terminal_task_event_id) > 0)
      ),
      terminal_task_event_sequence INTEGER CHECK (
        terminal_task_event_sequence IS NULL
        OR (typeof(terminal_task_event_sequence) = 'integer' AND terminal_task_event_sequence >= 1)
      ),
      terminal_task_event_stream_version INTEGER CHECK (
        terminal_task_event_stream_version IS NULL
        OR (
          typeof(terminal_task_event_stream_version) = 'integer'
          AND terminal_task_event_stream_version >= 1
        )
      ),
      mode_event_id TEXT CHECK (
        mode_event_id IS NULL OR (typeof(mode_event_id) = 'text' AND length(mode_event_id) > 0)
      ),
      mode_event_sequence INTEGER CHECK (
        mode_event_sequence IS NULL
        OR (typeof(mode_event_sequence) = 'integer' AND mode_event_sequence >= 1)
      ),
      mode_event_stream_version INTEGER CHECK (
        mode_event_stream_version IS NULL
        OR (typeof(mode_event_stream_version) = 'integer' AND mode_event_stream_version >= 1)
      ),
      recorded_at TEXT NOT NULL CHECK (
        typeof(recorded_at) = 'text'
        AND COALESCE(recorded_at = strftime('%Y-%m-%dT%H:%M:%fZ', recorded_at), 0)
      ),
      UNIQUE (run_id, ordinal),
      UNIQUE (run_id, step),
      FOREIGN KEY (run_id) REFERENCES agent_control_run_once_activations(run_id),
      FOREIGN KEY (receipt_id) REFERENCES agent_control_run_once_step_receipts(receipt_id)
        DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id) REFERENCES agent_control_run_once_step_markers(marker_id)
        DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (terminal_task_event_id) REFERENCES agent_control_events(event_id),
      FOREIGN KEY (mode_event_id) REFERENCES agent_control_events(event_id),
      CHECK (
        (terminal_task_event_id IS NULL) = (terminal_task_event_sequence IS NULL)
        AND (terminal_task_event_id IS NULL) = (terminal_task_event_stream_version IS NULL)
        AND (mode_event_id IS NULL) = (mode_event_sequence IS NULL)
        AND (mode_event_id IS NULL) = (mode_event_stream_version IS NULL)
      )
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_run_once_step_receipts (
      receipt_id TEXT PRIMARY KEY CHECK (typeof(receipt_id) = 'text' AND length(receipt_id) > 64),
      evidence_id TEXT NOT NULL UNIQUE CHECK (typeof(evidence_id) = 'text' AND length(evidence_id) > 64),
      marker_id TEXT NOT NULL UNIQUE CHECK (typeof(marker_id) = 'text' AND length(marker_id) > 64),
      run_id TEXT NOT NULL CHECK (typeof(run_id) = 'text' AND length(run_id) > 64),
      ordinal INTEGER NOT NULL CHECK (typeof(ordinal) = 'integer' AND ordinal >= 1),
      step TEXT NOT NULL CHECK (typeof(step) = 'text'),
      command_id TEXT NOT NULL UNIQUE CHECK (typeof(command_id) = 'text' AND length(command_id) > 64),
      status TEXT NOT NULL CHECK (typeof(status) = 'text' AND status = 'accepted'),
      accepted_at TEXT NOT NULL CHECK (
        typeof(accepted_at) = 'text'
        AND COALESCE(accepted_at = strftime('%Y-%m-%dT%H:%M:%fZ', accepted_at), 0)
      ),
      UNIQUE (run_id, ordinal),
      FOREIGN KEY (evidence_id) REFERENCES agent_control_run_once_step_evidence(evidence_id),
      FOREIGN KEY (marker_id) REFERENCES agent_control_run_once_step_markers(marker_id)
        DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (run_id) REFERENCES agent_control_run_once_activations(run_id)
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_run_once_step_markers (
      marker_id TEXT PRIMARY KEY CHECK (typeof(marker_id) = 'text' AND length(marker_id) > 64),
      evidence_id TEXT NOT NULL UNIQUE CHECK (typeof(evidence_id) = 'text' AND length(evidence_id) > 64),
      receipt_id TEXT NOT NULL UNIQUE CHECK (typeof(receipt_id) = 'text' AND length(receipt_id) > 64),
      run_id TEXT NOT NULL CHECK (typeof(run_id) = 'text' AND length(run_id) > 64),
      ordinal INTEGER NOT NULL CHECK (typeof(ordinal) = 'integer' AND ordinal >= 1),
      step TEXT NOT NULL CHECK (typeof(step) = 'text'),
      command_id TEXT NOT NULL UNIQUE CHECK (typeof(command_id) = 'text' AND length(command_id) > 64),
      marker_fingerprint TEXT NOT NULL UNIQUE CHECK (
        typeof(marker_fingerprint) = 'text' AND length(marker_fingerprint) = 64
        AND marker_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      committed_at TEXT NOT NULL CHECK (
        typeof(committed_at) = 'text'
        AND COALESCE(committed_at = strftime('%Y-%m-%dT%H:%M:%fZ', committed_at), 0)
      ),
      UNIQUE (run_id, ordinal),
      FOREIGN KEY (evidence_id) REFERENCES agent_control_run_once_step_evidence(evidence_id),
      FOREIGN KEY (receipt_id) REFERENCES agent_control_run_once_step_receipts(receipt_id),
      FOREIGN KEY (run_id) REFERENCES agent_control_run_once_activations(run_id)
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_run_once_states (
      run_id TEXT PRIMARY KEY CHECK (typeof(run_id) = 'text' AND length(run_id) > 64),
      project_id TEXT NOT NULL CHECK (typeof(project_id) = 'text' AND length(project_id) > 0),
      status TEXT NOT NULL CHECK (
        typeof(status) = 'text' AND status IN ('active', 'completed', 'no-eligible-task')
      ),
      next_ordinal INTEGER NOT NULL CHECK (typeof(next_ordinal) = 'integer' AND next_ordinal >= 2),
      last_step TEXT NOT NULL CHECK (typeof(last_step) = 'text'),
      task_id TEXT,
      stage_run_id TEXT,
      lease_id TEXT,
      worktree_reservation_id TEXT,
      controlled_thread_reservation_id TEXT,
      terminal_task_event_id TEXT,
      activation_project_revision INTEGER NOT NULL CHECK (
        typeof(activation_project_revision) = 'integer' AND activation_project_revision >= 1
      ),
      reset_project_revision INTEGER CHECK (
        reset_project_revision IS NULL
        OR (typeof(reset_project_revision) = 'integer' AND reset_project_revision >= 1)
      ),
      updated_at TEXT NOT NULL CHECK (
        typeof(updated_at) = 'text'
        AND COALESCE(updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', updated_at), 0)
      ),
      FOREIGN KEY (run_id) REFERENCES agent_control_run_once_activations(run_id),
      FOREIGN KEY (terminal_task_event_id) REFERENCES agent_control_events(event_id)
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX main.idx_agent_control_run_once_active_project
    ON agent_control_run_once_states(project_id) WHERE status = 'active'
  `;
  yield* sql`
    CREATE TABLE main.agent_control_run_once_step_claims (
      claim_id TEXT PRIMARY KEY CHECK (typeof(claim_id) = 'text' AND length(claim_id) > 64),
      run_id TEXT NOT NULL CHECK (typeof(run_id) = 'text' AND length(run_id) > 64),
      ordinal INTEGER NOT NULL CHECK (typeof(ordinal) = 'integer' AND ordinal >= 1),
      step TEXT NOT NULL CHECK (typeof(step) = 'text'),
      command_id TEXT NOT NULL UNIQUE CHECK (typeof(command_id) = 'text' AND length(command_id) > 64),
      claimed_at TEXT NOT NULL CHECK (
        typeof(claimed_at) = 'text'
        AND COALESCE(claimed_at = strftime('%Y-%m-%dT%H:%M:%fZ', claimed_at), 0)
      ),
      UNIQUE (run_id, ordinal),
      UNIQUE (run_id, step),
      FOREIGN KEY (run_id) REFERENCES agent_control_run_once_activations(run_id)
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_run_once_publications (
      publication_id TEXT PRIMARY KEY CHECK (
        typeof(publication_id) = 'text' AND length(publication_id) > 64
      ),
      marker_id TEXT NOT NULL UNIQUE CHECK (typeof(marker_id) = 'text' AND length(marker_id) > 64),
      evidence_id TEXT NOT NULL UNIQUE CHECK (typeof(evidence_id) = 'text' AND length(evidence_id) > 64),
      run_id TEXT NOT NULL CHECK (typeof(run_id) = 'text' AND length(run_id) > 64),
      ordinal INTEGER NOT NULL CHECK (typeof(ordinal) = 'integer' AND ordinal >= 1),
      step TEXT NOT NULL CHECK (typeof(step) = 'text'),
      published_at TEXT CHECK (
        published_at IS NULL OR (
          typeof(published_at) = 'text'
          AND COALESCE(published_at = strftime('%Y-%m-%dT%H:%M:%fZ', published_at), 0)
        )
      ),
      attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(attempt_count) = 'integer' AND attempt_count >= 0
      ),
      FOREIGN KEY (marker_id) REFERENCES agent_control_run_once_step_markers(marker_id)
        DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (evidence_id) REFERENCES agent_control_run_once_step_evidence(evidence_id),
      FOREIGN KEY (run_id) REFERENCES agent_control_run_once_activations(run_id)
    )
  `;
  yield* sql`
    CREATE INDEX main.idx_agent_control_run_once_recovery
    ON agent_control_run_once_states(status, project_id, run_id)
  `;
  yield* sql`
    CREATE INDEX main.idx_agent_control_run_once_publication_recovery
    ON agent_control_run_once_publications(published_at, run_id, ordinal)
  `;
  yield* sql`
    CREATE INDEX main.idx_agent_control_run_once_candidates
    ON agent_control_task_states(
      project_id, github_intake_sequence, status, source_gate, stage, issue_number, task_id
    )
  `;
});

const createTriggers = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_activation_identity_validate
    BEFORE INSERT ON agent_control_run_once_activations
    WHEN ${sql.literal(ACTIVATION_IDENTITY_MATCH)}(
      NEW.run_id, NEW.project_id, NEW.activation_event_id,
      NEW.activation_event_sequence, NEW.activation_event_stream_version,
      NEW.activation_command_id
    ) != 1
    BEGIN SELECT RAISE(ABORT, 'run-once activation identity is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_activation_event_validate
    BEFORE INSERT ON agent_control_run_once_activations
    WHEN NOT EXISTS (
        SELECT 1 FROM main.agent_control_events activation
        WHERE activation.event_id = NEW.activation_event_id
          AND activation.aggregate_kind = 'project-controller'
          AND activation.stream_id = NEW.project_id
          AND activation.event_type = 'agentControl.project.mode.changed'
          AND activation.actor_authority = 'human'
          AND activation.sequence = NEW.activation_event_sequence
          AND activation.stream_version = NEW.activation_event_stream_version
          AND activation.command_id = NEW.activation_command_id
          AND activation.correlation_id = NEW.activation_command_id
          AND activation.causation_event_id IS NULL
          AND json_extract(activation.payload_json, '$.previousMode') = 'observe'
          AND json_extract(activation.payload_json, '$.mode') = 'run-once'
          AND json_extract(activation.payload_json, '$.pausedFromMode') IS NULL
          AND json_extract(activation.payload_json, '$.changedAt') = NEW.activated_at
          AND activation.occurred_at = NEW.activated_at
    )
    BEGIN SELECT RAISE(ABORT, 'run-once activation event authority is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_activation_source_validate
    BEFORE INSERT ON agent_control_run_once_activations
    WHEN NOT EXISTS (
        SELECT 1 FROM main.agent_control_events github
        WHERE github.event_id = NEW.github_event_id
          AND github.aggregate_kind = 'github-intake'
          AND github.stream_id = NEW.project_id
          AND github.event_type = 'agentControl.github.poll.succeeded'
          AND github.sequence = NEW.github_event_sequence
          AND github.stream_version = NEW.github_event_stream_version
          AND github.sequence = NEW.github_intake_sequence
          AND github.sequence < NEW.activation_event_sequence
          AND json_extract(github.payload_json, '$.projectId') = NEW.project_id
          AND json_extract(github.payload_json, '$.completedAt') = github.occurred_at
          AND ${sql.literal(SOURCE_FINGERPRINT_MATCH)}(
            NEW.source_fingerprint, NEW.project_id, NEW.github_intake_sequence,
            NEW.github_event_stream_version, NEW.github_event_stream_version,
            json_extract(github.payload_json, '$.repository.repositoryNodeId'),
            json_array_length(json_extract(github.payload_json, '$.issues'))
          ) = 1
    )
    OR EXISTS (
      SELECT 1 FROM main.agent_control_events later
      WHERE later.aggregate_kind = 'github-intake' AND later.stream_id = NEW.project_id
        AND later.event_type = 'agentControl.github.poll.succeeded'
        AND later.sequence > NEW.github_event_sequence
        AND later.sequence < NEW.activation_event_sequence
    )
    BEGIN SELECT RAISE(ABORT, 'run-once activation source authority is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_activation_reconcile_validate
    BEFORE INSERT ON agent_control_run_once_activations
    WHEN NOT EXISTS (
        SELECT 1 FROM main.agent_control_task_reconcile_states reconcile
        WHERE reconcile.project_id = NEW.project_id
          AND reconcile.status = 'completed'
          AND reconcile.target_sequence = NEW.github_intake_sequence
          AND reconcile.last_completed_sequence = NEW.github_intake_sequence
          AND reconcile.revision = NEW.reconcile_revision
    )
    BEGIN SELECT RAISE(ABORT, 'run-once activation reconcile authority is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_activation_project_validate
    BEFORE INSERT ON agent_control_run_once_activations
    WHEN NOT EXISTS (
        SELECT 1 FROM main.agent_control_project_states project
        WHERE project.project_id = NEW.project_id AND project.mode = 'run-once'
          AND project.paused_from_mode IS NULL
          AND project.revision = NEW.activation_event_stream_version
          AND project.last_event_sequence = NEW.activation_event_sequence
          AND project.updated_at = NEW.activated_at
    )
    BEGIN SELECT RAISE(ABORT, 'run-once activation project authority is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_activation_exclusive_validate
    BEFORE INSERT ON agent_control_run_once_activations
    WHEN EXISTS (
        SELECT 1 FROM main.agent_control_run_once_states existing
        WHERE existing.project_id = NEW.project_id
          AND existing.status = 'active'
          AND existing.run_id != NEW.run_id
    )
    BEGIN SELECT RAISE(ABORT, 'run-once activation already has an active run'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_evidence_validate
    BEFORE INSERT ON agent_control_run_once_step_evidence
    WHEN NOT (
      ${sql.literal(CANONICAL_BLOB_MATCH)}(NEW.payload_json, NEW.payload_fingerprint) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'evidence', NEW.evidence_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'receipt', NEW.receipt_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'marker', NEW.marker_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'command', NEW.command_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND EXISTS (
        SELECT 1 FROM main.agent_control_run_once_activations activation
        WHERE activation.run_id = NEW.run_id AND activation.project_id = NEW.project_id
      )
      AND CASE NEW.step
        WHEN 'activation-admitted' THEN
          NEW.ordinal = 1 AND
          NEW.task_id IS NULL AND NEW.stage_run_id IS NULL AND NEW.lease_id IS NULL
          AND NEW.worktree_reservation_id IS NULL
          AND NEW.controlled_thread_reservation_id IS NULL
          AND NEW.terminal_task_event_id IS NULL AND NEW.mode_event_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM main.agent_control_run_once_states state
            WHERE state.run_id = NEW.run_id
          )
        WHEN 'task-selected' THEN
          NEW.task_id IS NOT NULL AND NEW.stage_run_id IS NULL AND NEW.lease_id IS NULL
          AND NEW.worktree_reservation_id IS NULL
          AND NEW.controlled_thread_reservation_id IS NULL
          AND NEW.terminal_task_event_id IS NULL AND NEW.mode_event_id IS NULL
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_step_evidence prior
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.evidence_id = prior.evidence_id
            WHERE prior.run_id = NEW.run_id AND prior.ordinal = NEW.ordinal - 1
              AND prior.step = 'activation-admitted'
          )
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_activations activation
            JOIN main.agent_control_task_states task ON task.task_id = NEW.task_id
            WHERE activation.run_id = NEW.run_id
              AND task.project_id = NEW.project_id
              AND task.github_intake_sequence = activation.github_intake_sequence
              AND task.status = 'candidate' AND task.source_gate = 'eligible'
              AND task.stage = 'intake'
          )
        WHEN 'no-eligible-task' THEN
          NEW.task_id IS NULL AND NEW.stage_run_id IS NULL AND NEW.lease_id IS NULL
          AND NEW.worktree_reservation_id IS NULL
          AND NEW.controlled_thread_reservation_id IS NULL
          AND NEW.terminal_task_event_id IS NULL AND NEW.mode_event_id IS NULL
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_step_evidence prior
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.evidence_id = prior.evidence_id
            WHERE prior.run_id = NEW.run_id AND prior.ordinal = NEW.ordinal - 1
              AND prior.step = 'activation-admitted'
          )
          AND NOT EXISTS (
            SELECT 1 FROM main.agent_control_task_states candidate
            JOIN main.agent_control_run_once_activations activation
              ON activation.run_id = NEW.run_id
            WHERE candidate.project_id = NEW.project_id
              AND candidate.github_intake_sequence = activation.github_intake_sequence
              AND candidate.status = 'candidate' AND candidate.source_gate = 'eligible'
              AND candidate.stage = 'intake'
          )
        WHEN 'stage-prepared' THEN
          NEW.task_id IS NOT NULL AND NEW.stage_run_id IS NOT NULL
          AND NEW.lease_id IS NULL AND NEW.worktree_reservation_id IS NULL
          AND NEW.controlled_thread_reservation_id IS NULL
          AND NEW.terminal_task_event_id IS NULL AND NEW.mode_event_id IS NULL
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_step_evidence prior
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.evidence_id = prior.evidence_id
            WHERE prior.run_id = NEW.run_id AND prior.ordinal = NEW.ordinal - 1
              AND prior.step = 'task-selected' AND prior.task_id = NEW.task_id
          )
        WHEN 'lease-reserved' THEN
          NEW.task_id IS NOT NULL AND NEW.stage_run_id IS NOT NULL AND NEW.lease_id IS NOT NULL
          AND NEW.worktree_reservation_id IS NULL
          AND NEW.controlled_thread_reservation_id IS NULL
          AND NEW.terminal_task_event_id IS NULL AND NEW.mode_event_id IS NULL
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_step_evidence prior
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.evidence_id = prior.evidence_id
            WHERE prior.run_id = NEW.run_id AND prior.ordinal = NEW.ordinal - 1
              AND prior.step = 'stage-prepared' AND prior.task_id = NEW.task_id
              AND prior.stage_run_id = NEW.stage_run_id
          )
        WHEN 'worktree-ready' THEN
          NEW.task_id IS NOT NULL AND NEW.stage_run_id IS NOT NULL AND NEW.lease_id IS NOT NULL
          AND NEW.worktree_reservation_id IS NOT NULL
          AND NEW.controlled_thread_reservation_id IS NULL
          AND NEW.terminal_task_event_id IS NULL AND NEW.mode_event_id IS NULL
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_step_evidence prior
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.evidence_id = prior.evidence_id
            WHERE prior.run_id = NEW.run_id AND prior.ordinal = NEW.ordinal - 1
              AND prior.step = 'lease-reserved' AND prior.task_id = NEW.task_id
              AND prior.stage_run_id = NEW.stage_run_id AND prior.lease_id = NEW.lease_id
          )
        WHEN 'thread-activated' THEN
          NEW.task_id IS NOT NULL AND NEW.stage_run_id IS NOT NULL AND NEW.lease_id IS NOT NULL
          AND NEW.worktree_reservation_id IS NOT NULL
          AND NEW.controlled_thread_reservation_id IS NOT NULL
          AND NEW.terminal_task_event_id IS NULL AND NEW.mode_event_id IS NULL
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_step_evidence prior
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.evidence_id = prior.evidence_id
            WHERE prior.run_id = NEW.run_id AND prior.ordinal = NEW.ordinal - 1
              AND prior.step = 'worktree-ready' AND prior.task_id = NEW.task_id
              AND prior.stage_run_id = NEW.stage_run_id AND prior.lease_id = NEW.lease_id
              AND prior.worktree_reservation_id = NEW.worktree_reservation_id
          )
        WHEN 'task-terminal-observed' THEN
          NEW.task_id IS NOT NULL AND NEW.stage_run_id IS NOT NULL AND NEW.lease_id IS NOT NULL
          AND NEW.worktree_reservation_id IS NOT NULL
          AND NEW.controlled_thread_reservation_id IS NOT NULL
          AND NEW.terminal_task_event_id IS NOT NULL
          AND NEW.terminal_task_event_sequence IS NOT NULL
          AND NEW.terminal_task_event_stream_version IS NOT NULL
          AND NEW.mode_event_id IS NULL
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_step_evidence prior
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.evidence_id = prior.evidence_id
            WHERE prior.run_id = NEW.run_id AND prior.ordinal = NEW.ordinal - 1
              AND prior.step = 'thread-activated' AND prior.task_id = NEW.task_id
              AND prior.stage_run_id = NEW.stage_run_id AND prior.lease_id = NEW.lease_id
              AND prior.worktree_reservation_id = NEW.worktree_reservation_id
              AND prior.controlled_thread_reservation_id = NEW.controlled_thread_reservation_id
          )
        WHEN 'mode-reset' THEN
          NEW.mode_event_id IS NOT NULL AND NEW.mode_event_sequence IS NOT NULL
          AND NEW.mode_event_stream_version IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_step_evidence prior
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.evidence_id = prior.evidence_id
            WHERE prior.run_id = NEW.run_id AND prior.ordinal = NEW.ordinal - 1
              AND prior.step IN ('no-eligible-task', 'task-terminal-observed')
              AND prior.task_id IS NEW.task_id AND prior.stage_run_id IS NEW.stage_run_id
              AND prior.lease_id IS NEW.lease_id
              AND prior.worktree_reservation_id IS NEW.worktree_reservation_id
              AND prior.controlled_thread_reservation_id IS NEW.controlled_thread_reservation_id
              AND prior.terminal_task_event_id IS NEW.terminal_task_event_id
          )
        WHEN 'mode-reset-superseded' THEN
          NEW.mode_event_id IS NULL
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_step_evidence prior
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.evidence_id = prior.evidence_id
            WHERE prior.run_id = NEW.run_id AND prior.ordinal = NEW.ordinal - 1
              AND prior.step IN ('no-eligible-task', 'task-terminal-observed')
              AND prior.task_id IS NEW.task_id AND prior.stage_run_id IS NEW.stage_run_id
              AND prior.lease_id IS NEW.lease_id
              AND prior.worktree_reservation_id IS NEW.worktree_reservation_id
              AND prior.controlled_thread_reservation_id IS NEW.controlled_thread_reservation_id
              AND prior.terminal_task_event_id IS NEW.terminal_task_event_id
          )
        WHEN 'completed' THEN
          NEW.mode_event_id IS NULL
          AND NEW.mode_event_sequence IS NULL
          AND NEW.mode_event_stream_version IS NULL
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_step_evidence prior
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.evidence_id = prior.evidence_id
            WHERE prior.run_id = NEW.run_id AND prior.ordinal = NEW.ordinal - 1
              AND prior.step IN ('mode-reset', 'mode-reset-superseded')
              AND prior.task_id IS NEW.task_id AND prior.stage_run_id IS NEW.stage_run_id
              AND prior.lease_id IS NEW.lease_id
              AND prior.worktree_reservation_id IS NEW.worktree_reservation_id
              AND prior.controlled_thread_reservation_id IS NEW.controlled_thread_reservation_id
              AND prior.terminal_task_event_id IS NEW.terminal_task_event_id
          )
        ELSE 0
      END
    )
    BEGIN SELECT RAISE(ABORT, 'run-once step evidence is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_downstream_evidence_validate
    BEFORE INSERT ON agent_control_run_once_step_evidence
    WHEN NEW.step IN ('stage-prepared', 'lease-reserved', 'worktree-ready', 'thread-activated')
      AND NOT CASE NEW.step
        WHEN 'stage-prepared' THEN EXISTS (
          SELECT 1
          FROM main.agent_control_command_receipts receipt
          JOIN main.agent_control_events event
            ON event.command_id = receipt.command_id
           AND event.aggregate_kind = 'stage-run'
           AND event.stream_id = NEW.stage_run_id
           AND event.event_type = 'agentControl.stageRun.prepared'
          JOIN main.agent_control_stage_run_states state
            ON state.stage_run_id = NEW.stage_run_id
          WHERE receipt.command_id = NEW.command_id
            AND receipt.authority = 'controller'
            AND receipt.aggregate_kind = 'stage-run'
            AND receipt.aggregate_id = NEW.stage_run_id
            AND receipt.status = 'accepted' AND receipt.event_created = 1
            AND receipt.result_sequence = event.sequence
            AND receipt.result_stream_version = event.stream_version
            AND event.stream_version = 1 AND event.actor_authority = 'controller'
            AND json_extract(event.payload_json, '$.projectId') = NEW.project_id
            AND json_extract(event.payload_json, '$.taskId') = NEW.task_id
            AND json_extract(event.payload_json, '$.stageRunId') = NEW.stage_run_id
            AND state.project_id = NEW.project_id AND state.task_id = NEW.task_id
            AND state.status = 'prepared' AND state.revision = 1
            AND state.last_event_sequence = event.sequence
        )
        WHEN 'lease-reserved' THEN EXISTS (
          SELECT 1
          FROM main.agent_control_command_receipts receipt
          JOIN main.agent_control_events event
            ON event.command_id = receipt.command_id
           AND event.aggregate_kind = 'stage-run-lease'
           AND event.stream_id = NEW.lease_id
           AND event.event_type = 'agentControl.stageRunLease.reserved'
          JOIN main.agent_control_stage_run_lease_states state ON state.lease_id = NEW.lease_id
          WHERE receipt.command_id = NEW.command_id
            AND receipt.authority = 'controller'
            AND receipt.aggregate_kind = 'stage-run-lease'
            AND receipt.aggregate_id = NEW.lease_id
            AND receipt.status = 'accepted' AND receipt.event_created = 1
            AND receipt.result_sequence = event.sequence
            AND receipt.result_stream_version = event.stream_version
            AND event.stream_version = 1 AND event.actor_authority = 'controller'
            AND json_extract(event.payload_json, '$.projectId') = NEW.project_id
            AND json_extract(event.payload_json, '$.taskId') = NEW.task_id
            AND json_extract(event.payload_json, '$.stageRunId') = NEW.stage_run_id
            AND json_extract(event.payload_json, '$.leaseId') = NEW.lease_id
            AND state.project_id = NEW.project_id AND state.task_id = NEW.task_id
            AND state.stage_run_id = NEW.stage_run_id
            AND state.status = 'reserved' AND state.revision = 1
            AND state.last_event_sequence = event.sequence
        )
        WHEN 'worktree-ready' THEN EXISTS (
          SELECT 1
          FROM main.agent_control_worktree_controller_operations operation
          JOIN main.agent_control_worktree_reservation_states state
            ON state.reservation_id = NEW.worktree_reservation_id
          WHERE operation.command_id = NEW.command_id
            AND operation.command_type = 'reserve-and-materialize'
            AND operation.status = 'accepted'
            AND operation.project_id = NEW.project_id
            AND operation.task_id = NEW.task_id
            AND operation.worktree_reservation_id = NEW.worktree_reservation_id
            AND operation.result_status = 'ready'
            AND operation.result_reservation_id = NEW.worktree_reservation_id
            AND operation.result_revision = state.revision
            AND operation.result_sequence = state.last_event_sequence
            AND operation.close_anchor_command_id = NEW.command_id
            AND operation.close_anchor_command_type = 'reserve-and-materialize'
            AND operation.close_anchor_project_id = NEW.project_id
            AND operation.close_anchor_task_id = NEW.task_id
            AND operation.close_anchor_reservation_id = NEW.worktree_reservation_id
            AND operation.close_anchor_phase = 'materialized'
            AND state.project_id = NEW.project_id AND state.task_id = NEW.task_id
            AND state.stage_run_id = NEW.stage_run_id AND state.lease_id = NEW.lease_id
            AND state.status = 'ready'
        )
        WHEN 'thread-activated' THEN EXISTS (
          SELECT 1
          FROM main.agent_control_command_receipts receipt
          JOIN main.agent_control_events initial
            ON initial.command_id = receipt.command_id
           AND initial.aggregate_kind = 'controlled-thread-reservation'
           AND initial.stream_id = NEW.controlled_thread_reservation_id
           AND initial.event_type = 'agentControl.controlledThreadReservation.prepared'
          JOIN main.agent_control_controlled_thread_reservation_states state
            ON state.controlled_thread_reservation_id = NEW.controlled_thread_reservation_id
          WHERE receipt.command_id = NEW.command_id
            AND receipt.authority = 'controller'
            AND receipt.aggregate_kind = 'controlled-thread-reservation'
            AND receipt.aggregate_id = NEW.controlled_thread_reservation_id
            AND receipt.status = 'accepted' AND receipt.event_created = 1
            AND receipt.result_sequence = initial.sequence
            AND receipt.result_stream_version = initial.stream_version
            AND initial.stream_version = 1 AND initial.actor_authority = 'controller'
            AND state.project_id = NEW.project_id AND state.task_id = NEW.task_id
            AND state.stage_run_id = NEW.stage_run_id AND state.lease_id = NEW.lease_id
            AND state.worktree_reservation_id = NEW.worktree_reservation_id
            AND state.status = 'bound' AND state.revision = 3
        )
        ELSE 0
      END
    BEGIN SELECT RAISE(ABORT, 'run-once downstream authority is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_terminal_evidence_validate
    BEFORE INSERT ON agent_control_run_once_step_evidence
    WHEN NEW.step = 'task-terminal-observed' AND NOT EXISTS (
      SELECT 1
      FROM main.agent_control_task_verification_finalization_evidence evidence
      JOIN main.agent_control_task_verification_finalization_receipts receipt
        ON receipt.receipt_id = evidence.receipt_id
       AND receipt.task_finalization_evidence_id = evidence.task_finalization_evidence_id
       AND receipt.marker_id = evidence.marker_id
       AND receipt.finalization_command_id = evidence.finalization_command_id
       AND receipt.finalization_fingerprint = evidence.finalization_fingerprint
       AND receipt.verification_marker_id = evidence.verification_marker_id
       AND receipt.handoff_id = evidence.handoff_id
       AND receipt.task_id = evidence.task_id
       AND receipt.task_event_id = evidence.task_event_id
       AND receipt.task_event_sequence = evidence.task_event_sequence
       AND receipt.task_event_stream_version = evidence.task_event_stream_version
       AND receipt.status = 'accepted'
       AND receipt.accepted_at = evidence.finalized_at
      JOIN main.agent_control_task_verification_finalization_markers marker
        ON marker.marker_id = evidence.marker_id
       AND marker.receipt_id = receipt.receipt_id
       AND marker.task_finalization_evidence_id = evidence.task_finalization_evidence_id
       AND marker.finalization_command_id = evidence.finalization_command_id
       AND marker.finalization_fingerprint = evidence.finalization_fingerprint
       AND marker.verification_marker_id = evidence.verification_marker_id
       AND marker.handoff_id = evidence.handoff_id
       AND marker.task_id = evidence.task_id
       AND marker.task_event_id = evidence.task_event_id
       AND marker.task_event_sequence = evidence.task_event_sequence
       AND marker.task_event_stream_version = evidence.task_event_stream_version
       AND marker.committed_at = evidence.finalized_at
      JOIN main.agent_control_events event
        ON event.event_id = evidence.task_event_id
       AND event.aggregate_kind = 'task'
       AND event.stream_id = evidence.task_id
       AND event.stream_version = evidence.task_event_stream_version
       AND event.sequence = evidence.task_event_sequence
      WHERE evidence.task_id = NEW.task_id
        AND evidence.project_id = NEW.project_id
        AND event.event_id = NEW.terminal_task_event_id
        AND event.sequence = NEW.terminal_task_event_sequence
        AND event.stream_version = NEW.terminal_task_event_stream_version
        AND event.event_type = 'agentControl.task.finalizedAfterVerification'
        AND event.actor_authority = 'system'
        AND event.command_id = evidence.finalization_command_id
        AND event.correlation_id = evidence.finalization_command_id
        AND json_extract(event.payload_json, '$.projectId') = NEW.project_id
        AND json_extract(event.payload_json, '$.taskId') = NEW.task_id
        AND json_extract(event.payload_json, '$.status') = evidence.verification_outcome
    )
    BEGIN SELECT RAISE(ABORT, 'run-once terminal Task authority is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_mode_evidence_validate
    BEFORE INSERT ON agent_control_run_once_step_evidence
    WHEN NEW.step = 'mode-reset' AND NOT EXISTS (
      SELECT 1 FROM main.agent_control_events event
      WHERE event.event_id = NEW.mode_event_id
        AND event.aggregate_kind = 'project-controller'
        AND event.stream_id = NEW.project_id
        AND event.event_type = 'agentControl.project.mode.changed'
        AND event.actor_authority = 'system'
        AND event.command_id = NEW.command_id
        AND event.correlation_id = NEW.command_id
        AND event.causation_event_id IS NULL
        AND event.sequence = NEW.mode_event_sequence
        AND event.stream_version = NEW.mode_event_stream_version
        AND json_extract(event.payload_json, '$.previousMode') = 'run-once'
        AND json_extract(event.payload_json, '$.mode') = 'observe'
        AND json_extract(event.payload_json, '$.previousPausedFromMode') IS NULL
        AND json_extract(event.payload_json, '$.pausedFromMode') IS NULL
        AND json_extract(event.payload_json, '$.changedAt') = event.occurred_at
        AND EXISTS (
          SELECT 1 FROM main.agent_control_project_states state
          WHERE state.project_id = NEW.project_id AND state.mode = 'observe'
            AND state.paused_from_mode IS NULL
            AND state.revision = event.stream_version
            AND state.last_event_sequence = event.sequence
        )
    )
    BEGIN SELECT RAISE(ABORT, 'run-once mode reset authority is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_mode_superseded_validate
    BEFORE INSERT ON agent_control_run_once_step_evidence
    WHEN NEW.step = 'mode-reset-superseded' AND NOT EXISTS (
      SELECT 1
      FROM main.agent_control_run_once_activations activation
      JOIN main.agent_control_project_states state ON state.project_id = NEW.project_id
      JOIN main.agent_control_events latest
        ON latest.aggregate_kind = 'project-controller'
       AND latest.stream_id = state.project_id
       AND latest.stream_version = state.revision
       AND latest.sequence = state.last_event_sequence
      WHERE activation.run_id = NEW.run_id
        AND state.mode = 'manual' AND state.paused_from_mode IS NULL
        AND latest.actor_authority = 'human'
        AND latest.event_type = 'agentControl.project.mode.changed'
        AND latest.sequence > activation.activation_event_sequence
        AND json_extract(latest.payload_json, '$.previousMode') IN ('run-once', 'paused')
        AND json_extract(latest.payload_json, '$.mode') = 'manual'
        AND json_extract(latest.payload_json, '$.pausedFromMode') IS NULL
    )
    BEGIN SELECT RAISE(ABORT, 'run-once supersession authority is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_receipt_validate
    BEFORE INSERT ON agent_control_run_once_step_receipts
    WHEN NOT (
      ${sql.literal(STEP_IDENTITY_MATCH)}(
        'receipt', NEW.receipt_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'evidence', NEW.evidence_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'marker', NEW.marker_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'command', NEW.command_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND EXISTS (
      SELECT 1 FROM main.agent_control_run_once_step_evidence evidence
      WHERE evidence.evidence_id = NEW.evidence_id
        AND evidence.receipt_id = NEW.receipt_id
        AND evidence.marker_id = NEW.marker_id
        AND evidence.run_id = NEW.run_id
        AND evidence.ordinal = NEW.ordinal
        AND evidence.step = NEW.step
        AND evidence.command_id = NEW.command_id
        AND NEW.status = 'accepted'
        AND evidence.recorded_at = NEW.accepted_at
      )
    )
    BEGIN SELECT RAISE(ABORT, 'run-once receipt is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_marker_validate
    BEFORE INSERT ON agent_control_run_once_step_markers
    WHEN NOT (
      ${sql.literal(STEP_IDENTITY_MATCH)}(
        'marker', NEW.marker_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'evidence', NEW.evidence_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'receipt', NEW.receipt_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'command', NEW.command_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND EXISTS (
      SELECT 1
      FROM main.agent_control_run_once_step_evidence evidence
      JOIN main.agent_control_run_once_step_receipts receipt
        ON receipt.receipt_id = evidence.receipt_id
       AND receipt.evidence_id = evidence.evidence_id
      JOIN main.agent_control_run_once_publications publication
        ON publication.marker_id = evidence.marker_id
       AND publication.evidence_id = evidence.evidence_id
      WHERE evidence.marker_id = NEW.marker_id
        AND evidence.evidence_id = NEW.evidence_id
        AND evidence.receipt_id = NEW.receipt_id
        AND evidence.run_id = NEW.run_id
        AND evidence.ordinal = NEW.ordinal
        AND evidence.step = NEW.step
        AND evidence.command_id = NEW.command_id
        AND receipt.marker_id = NEW.marker_id
        AND receipt.run_id = NEW.run_id
        AND receipt.ordinal = NEW.ordinal
        AND receipt.step = NEW.step
        AND receipt.command_id = NEW.command_id
        AND receipt.accepted_at = NEW.committed_at
        AND evidence.recorded_at = NEW.committed_at
        AND ${sql.literal(MARKER_MATCH)}(
          NEW.marker_fingerprint, NEW.run_id, NEW.ordinal, NEW.step,
          NEW.evidence_id, NEW.receipt_id, NEW.marker_id, NEW.command_id,
          evidence.payload_fingerprint, NEW.committed_at
        ) = 1
      )
    )
    BEGIN SELECT RAISE(ABORT, 'run-once marker is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_claim_validate
    BEFORE INSERT ON agent_control_run_once_step_claims
    WHEN NOT (
      ${sql.literal(STEP_IDENTITY_MATCH)}(
        'claim', NEW.claim_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND ${sql.literal(STEP_IDENTITY_MATCH)}(
        'command', NEW.command_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND EXISTS (
        SELECT 1 FROM main.agent_control_run_once_activations activation
        WHERE activation.run_id = NEW.run_id
      )
      AND (
        (NEW.ordinal = 1 AND NEW.step = 'activation-admitted'
          AND NOT EXISTS (
            SELECT 1 FROM main.agent_control_run_once_states state
            WHERE state.run_id = NEW.run_id
          ))
        OR EXISTS (
          SELECT 1 FROM main.agent_control_run_once_states state
          JOIN main.agent_control_run_once_step_markers prior
            ON prior.run_id = state.run_id AND prior.ordinal = NEW.ordinal - 1
          WHERE state.run_id = NEW.run_id AND state.status = 'active'
            AND state.next_ordinal = NEW.ordinal AND state.last_step = prior.step
        )
      )
    )
    BEGIN SELECT RAISE(ABORT, 'run-once claim is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_publication_validate
    BEFORE INSERT ON agent_control_run_once_publications
    WHEN NOT (
      ${sql.literal(STEP_IDENTITY_MATCH)}(
        'publication', NEW.publication_id, NEW.run_id, NEW.ordinal, NEW.step
      ) = 1
      AND EXISTS (
        SELECT 1 FROM main.agent_control_run_once_step_evidence evidence
        JOIN main.agent_control_run_once_step_receipts receipt
          ON receipt.receipt_id = evidence.receipt_id
         AND receipt.evidence_id = evidence.evidence_id
        WHERE evidence.marker_id = NEW.marker_id
          AND evidence.evidence_id = NEW.evidence_id
          AND evidence.run_id = NEW.run_id
          AND evidence.ordinal = NEW.ordinal
          AND evidence.step = NEW.step
          AND NEW.published_at IS NULL AND NEW.attempt_count = 0
      )
    )
    BEGIN SELECT RAISE(ABORT, 'run-once publication is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_state_insert_validate
    BEFORE INSERT ON agent_control_run_once_states
    WHEN NOT EXISTS (
      SELECT 1
      FROM main.agent_control_run_once_activations activation
      JOIN main.agent_control_run_once_step_evidence evidence
        ON evidence.run_id = activation.run_id
       AND evidence.ordinal = 1 AND evidence.step = 'activation-admitted'
      JOIN main.agent_control_run_once_step_receipts receipt
        ON receipt.evidence_id = evidence.evidence_id
      JOIN main.agent_control_run_once_publications publication
        ON publication.evidence_id = evidence.evidence_id
      WHERE NEW.run_id = activation.run_id AND NEW.project_id = activation.project_id
        AND NEW.status = 'active' AND NEW.next_ordinal = 2
        AND NEW.last_step = 'activation-admitted'
        AND NEW.task_id IS NULL AND NEW.stage_run_id IS NULL AND NEW.lease_id IS NULL
        AND NEW.worktree_reservation_id IS NULL
        AND NEW.controlled_thread_reservation_id IS NULL
        AND NEW.terminal_task_event_id IS NULL
        AND NEW.activation_project_revision = activation.activation_event_stream_version
        AND NEW.reset_project_revision IS NULL
        AND NEW.updated_at = evidence.recorded_at
    )
    BEGIN SELECT RAISE(ABORT, 'run-once initial state is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_state_update_validate
    BEFORE UPDATE ON agent_control_run_once_states
    WHEN NOT (
      NEW.run_id IS OLD.run_id AND NEW.project_id IS OLD.project_id
      AND OLD.status = 'active'
      AND NEW.activation_project_revision = OLD.activation_project_revision
      AND NEW.next_ordinal = OLD.next_ordinal + 1
      AND EXISTS (
        SELECT 1
        FROM main.agent_control_run_once_step_evidence evidence
        JOIN main.agent_control_run_once_step_receipts receipt
          ON receipt.evidence_id = evidence.evidence_id
        JOIN main.agent_control_run_once_publications publication
          ON publication.evidence_id = evidence.evidence_id
        WHERE evidence.run_id = OLD.run_id AND evidence.ordinal = OLD.next_ordinal
          AND evidence.step = NEW.last_step
          AND evidence.task_id IS NEW.task_id
          AND evidence.stage_run_id IS NEW.stage_run_id
          AND evidence.lease_id IS NEW.lease_id
          AND evidence.worktree_reservation_id IS NEW.worktree_reservation_id
          AND evidence.controlled_thread_reservation_id IS NEW.controlled_thread_reservation_id
          AND evidence.terminal_task_event_id IS NEW.terminal_task_event_id
          AND NEW.updated_at = evidence.recorded_at
          AND NEW.reset_project_revision IS CASE evidence.step
            WHEN 'mode-reset' THEN evidence.mode_event_stream_version
            WHEN 'mode-reset-superseded' THEN (
              SELECT project.revision FROM main.agent_control_project_states project
              WHERE project.project_id = OLD.project_id
            )
            ELSE OLD.reset_project_revision
          END
          AND NEW.status = CASE evidence.step
            WHEN 'completed' THEN CASE WHEN EXISTS (
              SELECT 1 FROM main.agent_control_run_once_step_evidence terminal
              WHERE terminal.run_id = OLD.run_id AND terminal.step = 'no-eligible-task'
            ) THEN 'no-eligible-task' ELSE 'completed' END
            ELSE 'active'
          END
      )
    )
    BEGIN SELECT RAISE(ABORT, 'run-once state transition is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_state_no_delete
    BEFORE DELETE ON agent_control_run_once_states
    BEGIN SELECT RAISE(ABORT, 'run-once state is immutable'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_publication_update
    BEFORE UPDATE ON agent_control_run_once_publications
    WHEN NOT (
      NEW.publication_id IS OLD.publication_id AND NEW.marker_id IS OLD.marker_id
      AND NEW.evidence_id IS OLD.evidence_id AND NEW.run_id IS OLD.run_id
      AND NEW.ordinal IS OLD.ordinal AND NEW.step IS OLD.step
      AND OLD.published_at IS NULL AND typeof(NEW.published_at) = 'text'
      AND NEW.attempt_count = OLD.attempt_count + 1
    )
    BEGIN SELECT RAISE(ABORT, 'run-once publication update is invalid'); END
  `;
  for (const table of [
    "agent_control_run_once_activations",
    "agent_control_run_once_step_evidence",
    "agent_control_run_once_step_receipts",
    "agent_control_run_once_step_markers",
    "agent_control_run_once_step_claims",
  ]) {
    yield* sql.unsafe(`
      CREATE TRIGGER main.${quote(`${table}_no_update`)}
      BEFORE UPDATE ON ${quote(table)}
      BEGIN SELECT RAISE(ABORT, 'run-once authority is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.${quote(`${table}_no_delete`)}
      BEFORE DELETE ON ${quote(table)}
      BEGIN SELECT RAISE(ABORT, 'run-once authority is immutable'); END
    `).unprepared;
  }
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_publication_no_delete
    BEFORE DELETE ON agent_control_run_once_publications
    BEGIN SELECT RAISE(ABORT, 'run-once publication is immutable'); END
  `;
});

export const makeMigration063 = (
  injectFault: (point: Migration063FaultPoint) => Effect.Effect<void> = () => Effect.void,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const collisions = yield* sql<{ readonly name: string }>`
      SELECT name FROM main.sqlite_schema
      WHERE name LIKE 'agent_control_run_once_%'
         OR name = 'idx_agent_control_run_once_candidates'
      ORDER BY name
    `;
    if (collisions.length !== 0) {
      return yield* Effect.die(new Error("migration 063 encountered partial or legacy objects"));
    }
    const fingerprint = sha256Utf8('{"schemaVersion":1}');
    const preflight = yield* sql<{
      readonly blob: number;
      readonly text: number;
      readonly divergent: number;
    }>`
      SELECT
        ${sql.literal(CANONICAL_BLOB_MATCH)}(
          CAST('{"schemaVersion":1}' AS BLOB), ${fingerprint}
        ) AS blob,
        ${sql.literal(CANONICAL_BLOB_MATCH)}('{"schemaVersion":1}', ${fingerprint}) AS text,
        ${sql.literal(CANONICAL_BLOB_MATCH)}(
          CAST('{"schemaVersion":1}' AS BLOB), ${"f".repeat(64)}
        ) AS divergent
    `;
    if (
      preflight.length !== 1 ||
      preflight[0]?.blob !== 1 ||
      preflight[0]?.text !== 0 ||
      preflight[0]?.divergent !== 0
    ) {
      return yield* Effect.die(new Error("migration 063 requires canonical BLOB authority UDF"));
    }
    yield* sql`PRAGMA defer_foreign_keys = ON`;
    yield* injectFault("before-project-state-rebuild");
    yield* rebuildProjectStates;
    yield* injectFault("after-project-state-rebuild");
    yield* createTables;
    yield* injectFault("after-run-once-tables");
    yield* createTriggers;
    yield* injectFault("after-run-once-triggers");
    const foreignKeys = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
    if (foreignKeys.length !== 0) {
      return yield* Effect.die(new Error("migration 063 introduced foreign-key violations"));
    }
    const integrity = yield* sql<{ readonly integrity_check: string }>`PRAGMA integrity_check`;
    if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") {
      return yield* Effect.die(new Error("migration 063 failed integrity_check"));
    }
    yield* sql`PRAGMA defer_foreign_keys = OFF`;
  });

export default makeMigration063();
