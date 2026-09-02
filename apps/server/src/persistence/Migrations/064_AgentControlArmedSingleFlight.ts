import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { sha256Utf8 } from "../../agentControl/initialPlanning/eventEvidence.ts";

const CANONICAL_BLOB_MATCH = "t3_run_once_canonical_blob_match";
const SOURCE_FINGERPRINT_MATCH = "t3_run_once_source_fingerprint_match";
const MODE_COMMAND_FINGERPRINT_MATCH = "t3_run_once_mode_command_fingerprint_match";
const MODE_EVENT_MATCH = "t3_run_once_mode_event_match";
const RUN_ONCE_STEP_IDENTITY_MATCH = "t3_run_once_step_identity_match";
const canonicalModeEventProbe =
  '{"changedAt":"2026-01-01T00:00:00.000Z","mode":"run-once","pausedFromMode":null,"previousMode":"armed","previousPausedFromMode":null,"projectId":"invalid"}';

interface SchemaObject {
  readonly name: string;
  readonly sql: string;
}

const EXPECTED_063_DDL_FINGERPRINTS: ReadonlyMap<string, string> = new Map([
  [
    "idx_agent_control_project_states_sequence",
    "a51422bede7c7fc118944b687dabf9aac368bdfd8b1bae036d0a8c284cfb52e8",
  ],
  [
    "idx_agent_control_run_once_candidates",
    "27641587cf24c0b2fa2cc5afe80071c1bdd4351abedea7716890eab2154bf566",
  ],
  [
    "agent_control_project_states",
    "c37258c881c09d1bf1790fdf7909beae0ff83baf286dbcc9f0ba08cca5a90ae5",
  ],
  [
    "agent_control_run_once_activations",
    "c31369d53e016f6b12d3a212d7750dc3d974b41a61d31c0ae703691b0201f133",
  ],
  [
    "agent_control_run_once_activation_event_validate",
    "fce4a5f63e86792110e12325798dd89648a9849442b7e8bb21cb89e7ad336793",
  ],
  [
    "agent_control_run_once_activation_project_validate",
    "77c9c0a7a36e5da73cfbe52bda4f64bfd11d687219e2991d827b3209bd968d03",
  ],
  [
    "agent_control_run_once_active_lineage_validate",
    "3cb0ddd2f6431a2343fde78d678da7a3d74a64b61b31154dd08011b71f7342d1",
  ],
  [
    "agent_control_run_once_evidence_validate",
    "3897023c2e3070412d0f6fc3715a4767fc85c3c8e1cf1700440a20090ef984c2",
  ],
  [
    "agent_control_run_once_mode_evidence_validate",
    "cb0ddfa8f3a028a1d6e130f83a687de29957baa1af09ba7051b930643128f6cb",
  ],
] as const);

const quote = (identifier: string) => `"${identifier.replaceAll('"', '""')}"`;
const restoreSchema = (objects: ReadonlyArray<SchemaObject>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const object of objects) yield* sql.unsafe(object.sql).unprepared;
  });

export type Migration064FaultPoint =
  | "before-project-state-rebuild"
  | "after-project-state-rebuild"
  | "after-armed-tables"
  | "after-authority-triggers";

const rebuildProjectStates = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [triggers, indexes] = yield* Effect.all([
    sql<SchemaObject>`
      SELECT name, sql FROM main.sqlite_schema
      WHERE type = 'trigger' AND sql IS NOT NULL
        AND (
          tbl_name = 'agent_control_project_states'
          OR instr(sql, 'agent_control_project_states') > 0
        )
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
    CREATE TABLE main.agent_control_project_states_rebuild_064 (
      project_id TEXT PRIMARY KEY CHECK (typeof(project_id) = 'text' AND length(project_id) > 0),
      mode TEXT NOT NULL CHECK (
        typeof(mode) = 'text' AND mode IN ('manual', 'observe', 'armed', 'run-once', 'paused')
      ),
      paused_from_mode TEXT CHECK (
        paused_from_mode IS NULL OR (
          typeof(paused_from_mode) = 'text'
          AND paused_from_mode IN ('observe', 'armed', 'run-once')
        )
      ),
      revision INTEGER NOT NULL CHECK (typeof(revision) = 'integer' AND revision >= 1),
      last_event_sequence INTEGER NOT NULL CHECK (
        typeof(last_event_sequence) = 'integer' AND last_event_sequence >= 1
      ),
      updated_at TEXT NOT NULL CHECK (
        typeof(updated_at) = 'text'
        AND COALESCE(updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', updated_at), 0)
      ),
      CHECK (
        (mode = 'paused' AND paused_from_mode IN ('observe', 'armed', 'run-once'))
        OR (mode != 'paused' AND paused_from_mode IS NULL)
      )
    )
  `;
  yield* sql`
    INSERT INTO main.agent_control_project_states_rebuild_064
    SELECT * FROM main.agent_control_project_states
  `;
  yield* sql`DROP TABLE main.agent_control_project_states`;
  yield* sql`
    ALTER TABLE main.agent_control_project_states_rebuild_064
    RENAME TO agent_control_project_states
  `;
  yield* restoreSchema(indexes);
  yield* restoreSchema(triggers);
});

const extendRunOnceActivations = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    ALTER TABLE main.agent_control_run_once_activations
    ADD COLUMN armed_dispatch_id TEXT CHECK (
      armed_dispatch_id IS NULL OR (typeof(armed_dispatch_id) = 'text' AND length(armed_dispatch_id) > 64)
    )
  `;
  yield* sql`
    ALTER TABLE main.agent_control_run_once_activations
    ADD COLUMN armed_claim_id TEXT CHECK (
      armed_claim_id IS NULL OR (typeof(armed_claim_id) = 'text' AND length(armed_claim_id) > 64)
    )
  `;
  yield* sql`
    ALTER TABLE main.agent_control_run_once_activations
    ADD COLUMN armed_marker_id TEXT CHECK (
      armed_marker_id IS NULL OR (typeof(armed_marker_id) = 'text' AND length(armed_marker_id) > 64)
    )
  `;
  yield* sql`
    ALTER TABLE main.agent_control_run_once_activations
    ADD COLUMN origin_mode TEXT NOT NULL DEFAULT 'observe' CHECK (
      typeof(origin_mode) = 'text'
      AND origin_mode IN ('observe', 'armed')
      AND (
        (origin_mode = 'observe' AND armed_dispatch_id IS NULL
          AND armed_claim_id IS NULL AND armed_marker_id IS NULL)
        OR
        (origin_mode = 'armed' AND armed_dispatch_id IS NOT NULL
          AND armed_claim_id IS NOT NULL AND armed_marker_id IS NOT NULL)
      )
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX main.idx_agent_control_run_once_armed_dispatch
    ON agent_control_run_once_activations(armed_dispatch_id)
    WHERE armed_dispatch_id IS NOT NULL
  `;
});

const createArmedTables = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE main.agent_control_armed_dispatch_evidence (
      evidence_id TEXT PRIMARY KEY CHECK (typeof(evidence_id) = 'text' AND length(evidence_id) > 64),
      receipt_id TEXT NOT NULL UNIQUE CHECK (typeof(receipt_id) = 'text' AND length(receipt_id) > 64),
      marker_id TEXT NOT NULL UNIQUE CHECK (typeof(marker_id) = 'text' AND length(marker_id) > 64),
      dispatch_id TEXT NOT NULL UNIQUE CHECK (typeof(dispatch_id) = 'text' AND length(dispatch_id) > 64),
      claim_id TEXT NOT NULL UNIQUE CHECK (typeof(claim_id) = 'text' AND length(claim_id) > 64),
      mode_command_id TEXT NOT NULL UNIQUE CHECK (
        typeof(mode_command_id) = 'text' AND length(mode_command_id) > 64
      ),
      project_id TEXT NOT NULL CHECK (typeof(project_id) = 'text' AND length(project_id) > 0),
      selected_task_id TEXT NOT NULL CHECK (
        typeof(selected_task_id) = 'text' AND length(selected_task_id) > 0
      ),
      project_revision INTEGER NOT NULL CHECK (
        typeof(project_revision) = 'integer' AND project_revision >= 1
      ),
      project_event_sequence INTEGER NOT NULL CHECK (
        typeof(project_event_sequence) = 'integer' AND project_event_sequence >= 1
      ),
      github_intake_sequence INTEGER NOT NULL CHECK (
        typeof(github_intake_sequence) = 'integer' AND github_intake_sequence >= 1
      ),
      github_event_id TEXT NOT NULL CHECK (typeof(github_event_id) = 'text' AND length(github_event_id) > 0),
      github_event_sequence INTEGER NOT NULL CHECK (
        typeof(github_event_sequence) = 'integer' AND github_event_sequence >= 1
      ),
      github_event_stream_version INTEGER NOT NULL CHECK (
        typeof(github_event_stream_version) = 'integer' AND github_event_stream_version >= 1
      ),
      source_fingerprint TEXT NOT NULL CHECK (
        typeof(source_fingerprint) = 'text' AND length(source_fingerprint) = 64
        AND source_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      reconcile_revision INTEGER NOT NULL CHECK (
        typeof(reconcile_revision) = 'integer' AND reconcile_revision >= 1
      ),
      task_frontier_sequence INTEGER NOT NULL CHECK (
        typeof(task_frontier_sequence) = 'integer' AND task_frontier_sequence >= 0
      ),
      task_frontier_revision INTEGER NOT NULL CHECK (
        typeof(task_frontier_revision) = 'integer' AND task_frontier_revision >= 0
      ),
      task_frontier_count INTEGER NOT NULL CHECK (
        typeof(task_frontier_count) = 'integer' AND task_frontier_count >= 0
      ),
      task_frontier_fingerprint TEXT NOT NULL CHECK (
        typeof(task_frontier_fingerprint) = 'text' AND length(task_frontier_fingerprint) = 64
        AND task_frontier_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      owner_id TEXT NOT NULL CHECK (typeof(owner_id) = 'text' AND length(owner_id) > 0),
      fence_token INTEGER NOT NULL CHECK (typeof(fence_token) = 'integer' AND fence_token >= 1),
      claimed_at TEXT NOT NULL CHECK (
        typeof(claimed_at) = 'text'
        AND COALESCE(claimed_at = strftime('%Y-%m-%dT%H:%M:%fZ', claimed_at), 0)
      ),
      expires_at TEXT NOT NULL CHECK (
        typeof(expires_at) = 'text'
        AND COALESCE(expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', expires_at), 0)
        AND expires_at > claimed_at
      ),
      payload_json BLOB NOT NULL CHECK (typeof(payload_json) = 'blob'),
      payload_fingerprint TEXT NOT NULL CHECK (
        typeof(payload_fingerprint) = 'text' AND length(payload_fingerprint) = 64
        AND payload_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      UNIQUE (project_id, project_revision, github_event_id, reconcile_revision,
        task_frontier_sequence, task_frontier_revision, task_frontier_count,
        task_frontier_fingerprint, selected_task_id, fence_token),
      FOREIGN KEY (github_event_id) REFERENCES agent_control_events(event_id),
      FOREIGN KEY (selected_task_id) REFERENCES agent_control_task_states(task_id),
      FOREIGN KEY (receipt_id) REFERENCES agent_control_armed_dispatch_receipts(receipt_id)
        DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id) REFERENCES agent_control_armed_dispatch_markers(marker_id)
        DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_armed_dispatch_receipts (
      receipt_id TEXT PRIMARY KEY CHECK (typeof(receipt_id) = 'text' AND length(receipt_id) > 64),
      evidence_id TEXT NOT NULL UNIQUE CHECK (typeof(evidence_id) = 'text' AND length(evidence_id) > 64),
      marker_id TEXT NOT NULL UNIQUE CHECK (typeof(marker_id) = 'text' AND length(marker_id) > 64),
      dispatch_id TEXT NOT NULL UNIQUE CHECK (typeof(dispatch_id) = 'text' AND length(dispatch_id) > 64),
      claim_id TEXT NOT NULL UNIQUE CHECK (typeof(claim_id) = 'text' AND length(claim_id) > 64),
      status TEXT NOT NULL CHECK (typeof(status) = 'text' AND status = 'accepted'),
      accepted_at TEXT NOT NULL CHECK (
        typeof(accepted_at) = 'text'
        AND COALESCE(accepted_at = strftime('%Y-%m-%dT%H:%M:%fZ', accepted_at), 0)
      ),
      FOREIGN KEY (evidence_id) REFERENCES agent_control_armed_dispatch_evidence(evidence_id),
      FOREIGN KEY (marker_id) REFERENCES agent_control_armed_dispatch_markers(marker_id)
        DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_armed_dispatch_markers (
      marker_id TEXT PRIMARY KEY CHECK (typeof(marker_id) = 'text' AND length(marker_id) > 64),
      evidence_id TEXT NOT NULL UNIQUE CHECK (typeof(evidence_id) = 'text' AND length(evidence_id) > 64),
      receipt_id TEXT NOT NULL UNIQUE CHECK (typeof(receipt_id) = 'text' AND length(receipt_id) > 64),
      dispatch_id TEXT NOT NULL UNIQUE CHECK (typeof(dispatch_id) = 'text' AND length(dispatch_id) > 64),
      claim_id TEXT NOT NULL UNIQUE CHECK (typeof(claim_id) = 'text' AND length(claim_id) > 64),
      marker_fingerprint TEXT NOT NULL CHECK (
        typeof(marker_fingerprint) = 'text' AND length(marker_fingerprint) = 64
        AND marker_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      committed_at TEXT NOT NULL CHECK (
        typeof(committed_at) = 'text'
        AND COALESCE(committed_at = strftime('%Y-%m-%dT%H:%M:%fZ', committed_at), 0)
      ),
      FOREIGN KEY (evidence_id) REFERENCES agent_control_armed_dispatch_evidence(evidence_id),
      FOREIGN KEY (receipt_id) REFERENCES agent_control_armed_dispatch_receipts(receipt_id)
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_armed_dispatch_states (
      dispatch_id TEXT PRIMARY KEY CHECK (typeof(dispatch_id) = 'text' AND length(dispatch_id) > 64),
      project_id TEXT NOT NULL CHECK (typeof(project_id) = 'text' AND length(project_id) > 0),
      status TEXT NOT NULL CHECK (
        typeof(status) = 'text' AND status IN ('claimed', 'activated', 'completed', 'superseded')
      ),
      owner_id TEXT NOT NULL CHECK (typeof(owner_id) = 'text' AND length(owner_id) > 0),
      fence_token INTEGER NOT NULL CHECK (typeof(fence_token) = 'integer' AND fence_token >= 1),
      expires_at TEXT NOT NULL CHECK (
        typeof(expires_at) = 'text'
        AND COALESCE(expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', expires_at), 0)
      ),
      activation_event_id TEXT,
      activation_event_sequence INTEGER,
      activation_event_stream_version INTEGER,
      updated_at TEXT NOT NULL CHECK (
        typeof(updated_at) = 'text'
        AND COALESCE(updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', updated_at), 0)
      ),
      CHECK (
        (activation_event_id IS NULL) = (activation_event_sequence IS NULL)
        AND (activation_event_id IS NULL) = (activation_event_stream_version IS NULL)
        AND (
          activation_event_id IS NULL
          OR (
            typeof(activation_event_id) = 'text' AND length(activation_event_id) > 0
            AND typeof(activation_event_sequence) = 'integer' AND activation_event_sequence >= 1
            AND typeof(activation_event_stream_version) = 'integer'
            AND activation_event_stream_version >= 1
          )
        )
      ),
      FOREIGN KEY (dispatch_id) REFERENCES agent_control_armed_dispatch_evidence(dispatch_id),
      FOREIGN KEY (activation_event_id) REFERENCES agent_control_events(event_id)
    )
  `;
  yield* sql`
    CREATE UNIQUE INDEX main.idx_agent_control_armed_active_project
    ON agent_control_armed_dispatch_states(project_id)
    WHERE status IN ('claimed', 'activated')
  `;
  yield* sql`
    CREATE INDEX main.idx_agent_control_armed_claim_recovery
    ON agent_control_armed_dispatch_states(status, expires_at, project_id, dispatch_id)
  `;

  yield* sql`
    CREATE TABLE main.agent_control_armed_no_candidate_evidence (
      evidence_id TEXT PRIMARY KEY CHECK (typeof(evidence_id) = 'text' AND length(evidence_id) > 64),
      receipt_id TEXT NOT NULL UNIQUE CHECK (typeof(receipt_id) = 'text' AND length(receipt_id) > 64),
      marker_id TEXT NOT NULL UNIQUE CHECK (typeof(marker_id) = 'text' AND length(marker_id) > 64),
      project_id TEXT NOT NULL CHECK (typeof(project_id) = 'text' AND length(project_id) > 0),
      project_revision INTEGER NOT NULL CHECK (typeof(project_revision) = 'integer' AND project_revision >= 1),
      project_event_sequence INTEGER NOT NULL CHECK (
        typeof(project_event_sequence) = 'integer' AND project_event_sequence >= 1
      ),
      github_intake_sequence INTEGER NOT NULL CHECK (
        typeof(github_intake_sequence) = 'integer' AND github_intake_sequence >= 1
      ),
      github_event_id TEXT NOT NULL CHECK (typeof(github_event_id) = 'text' AND length(github_event_id) > 0),
      github_event_sequence INTEGER NOT NULL CHECK (
        typeof(github_event_sequence) = 'integer' AND github_event_sequence >= 1
      ),
      github_event_stream_version INTEGER NOT NULL CHECK (
        typeof(github_event_stream_version) = 'integer' AND github_event_stream_version >= 1
      ),
      source_fingerprint TEXT NOT NULL CHECK (
        typeof(source_fingerprint) = 'text' AND length(source_fingerprint) = 64
        AND source_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      reconcile_revision INTEGER NOT NULL CHECK (
        typeof(reconcile_revision) = 'integer' AND reconcile_revision >= 1
      ),
      task_frontier_sequence INTEGER NOT NULL CHECK (
        typeof(task_frontier_sequence) = 'integer' AND task_frontier_sequence >= 0
      ),
      task_frontier_revision INTEGER NOT NULL CHECK (
        typeof(task_frontier_revision) = 'integer' AND task_frontier_revision >= 0
      ),
      task_frontier_count INTEGER NOT NULL CHECK (
        typeof(task_frontier_count) = 'integer' AND task_frontier_count >= 0
      ),
      task_frontier_fingerprint TEXT NOT NULL CHECK (
        typeof(task_frontier_fingerprint) = 'text' AND length(task_frontier_fingerprint) = 64
        AND task_frontier_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      decided_at TEXT NOT NULL CHECK (
        typeof(decided_at) = 'text'
        AND COALESCE(decided_at = strftime('%Y-%m-%dT%H:%M:%fZ', decided_at), 0)
      ),
      payload_json BLOB NOT NULL CHECK (typeof(payload_json) = 'blob'),
      payload_fingerprint TEXT NOT NULL CHECK (
        typeof(payload_fingerprint) = 'text' AND length(payload_fingerprint) = 64
        AND payload_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      UNIQUE (project_id, github_intake_sequence, github_event_id, github_event_sequence,
        github_event_stream_version, source_fingerprint, reconcile_revision,
        task_frontier_sequence, task_frontier_revision, task_frontier_count,
        task_frontier_fingerprint),
      FOREIGN KEY (github_event_id) REFERENCES agent_control_events(event_id),
      FOREIGN KEY (receipt_id) REFERENCES agent_control_armed_no_candidate_receipts(receipt_id)
        DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id) REFERENCES agent_control_armed_no_candidate_markers(marker_id)
        DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_armed_no_candidate_receipts (
      receipt_id TEXT PRIMARY KEY CHECK (typeof(receipt_id) = 'text' AND length(receipt_id) > 64),
      evidence_id TEXT NOT NULL UNIQUE CHECK (typeof(evidence_id) = 'text' AND length(evidence_id) > 64),
      marker_id TEXT NOT NULL UNIQUE CHECK (typeof(marker_id) = 'text' AND length(marker_id) > 64),
      status TEXT NOT NULL CHECK (typeof(status) = 'text' AND status = 'accepted'),
      accepted_at TEXT NOT NULL CHECK (
        typeof(accepted_at) = 'text'
        AND COALESCE(accepted_at = strftime('%Y-%m-%dT%H:%M:%fZ', accepted_at), 0)
      ),
      FOREIGN KEY (evidence_id) REFERENCES agent_control_armed_no_candidate_evidence(evidence_id),
      FOREIGN KEY (marker_id) REFERENCES agent_control_armed_no_candidate_markers(marker_id)
        DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_armed_no_candidate_markers (
      marker_id TEXT PRIMARY KEY CHECK (typeof(marker_id) = 'text' AND length(marker_id) > 64),
      evidence_id TEXT NOT NULL UNIQUE CHECK (typeof(evidence_id) = 'text' AND length(evidence_id) > 64),
      receipt_id TEXT NOT NULL UNIQUE CHECK (typeof(receipt_id) = 'text' AND length(receipt_id) > 64),
      marker_fingerprint TEXT NOT NULL CHECK (
        typeof(marker_fingerprint) = 'text' AND length(marker_fingerprint) = 64
        AND marker_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      committed_at TEXT NOT NULL CHECK (
        typeof(committed_at) = 'text'
        AND COALESCE(committed_at = strftime('%Y-%m-%dT%H:%M:%fZ', committed_at), 0)
      ),
      FOREIGN KEY (evidence_id) REFERENCES agent_control_armed_no_candidate_evidence(evidence_id),
      FOREIGN KEY (receipt_id) REFERENCES agent_control_armed_no_candidate_receipts(receipt_id)
    )
  `;
  yield* sql`
    CREATE INDEX main.idx_agent_control_armed_no_candidate_catchup
    ON agent_control_armed_no_candidate_evidence(project_id, github_event_sequence,
      reconcile_revision, task_frontier_sequence, task_frontier_revision)
  `;
  yield* sql`
    CREATE INDEX main.idx_agent_control_armed_task_frontier
    ON agent_control_task_states(project_id, task_id, last_event_sequence, revision)
  `;
  yield* sql`
    CREATE INDEX main.idx_agent_control_armed_project_catchup
    ON agent_control_project_states(mode, paused_from_mode, project_id)
    WHERE mode = 'armed' AND paused_from_mode IS NULL
  `;
});

const createArmedTriggers = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TRIGGER main.agent_control_armed_dispatch_evidence_validate
    BEFORE INSERT ON agent_control_armed_dispatch_evidence
    WHEN NOT (
      ${sql.literal(CANONICAL_BLOB_MATCH)}(NEW.payload_json, NEW.payload_fingerprint) = 1
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.schemaVersion') = 1
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.kind') = 'dispatch'
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.evidenceId') = NEW.evidence_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.receiptId') = NEW.receipt_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.markerId') = NEW.marker_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.dispatchId') = NEW.dispatch_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.claimId') = NEW.claim_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.commandId') = NEW.mode_command_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.projectId') = NEW.project_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.selectedTaskId') = NEW.selected_task_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.projectRevision') = NEW.project_revision
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.projectEventSequence') = NEW.project_event_sequence
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.githubIntakeSequence') = NEW.github_intake_sequence
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.githubEventId') = NEW.github_event_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.githubEventSequence') = NEW.github_event_sequence
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.githubEventStreamVersion') = NEW.github_event_stream_version
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.sourceFingerprint') = NEW.source_fingerprint
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.reconcileRevision') = NEW.reconcile_revision
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.taskFrontierSequence') = NEW.task_frontier_sequence
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.taskFrontierRevision') = NEW.task_frontier_revision
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.taskFrontierCount') = NEW.task_frontier_count
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.taskFrontierFingerprint') = NEW.task_frontier_fingerprint
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.ownerId') = NEW.owner_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.fenceToken') = NEW.fence_token
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.claimedAt') = NEW.claimed_at
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.expiresAt') = NEW.expires_at
      AND json_type(CAST(NEW.payload_json AS TEXT), '$.taskFrontier') = 'array'
      AND json_array_length(json_extract(CAST(NEW.payload_json AS TEXT), '$.taskFrontier')) = NEW.task_frontier_count
      AND EXISTS (
        SELECT 1 FROM main.agent_control_project_states project
        WHERE project.project_id = NEW.project_id AND project.mode = 'armed'
          AND project.paused_from_mode IS NULL
          AND project.revision = NEW.project_revision
          AND project.last_event_sequence = NEW.project_event_sequence
      )
      AND EXISTS (
        SELECT 1 FROM main.agent_control_github_intake_states projected
        WHERE projected.project_id = NEW.project_id
          AND projected.last_event_sequence = NEW.github_intake_sequence
          AND projected.revision = NEW.github_event_stream_version
          AND json_extract(projected.state_json, '$.config.revision') = NEW.github_event_stream_version
          AND json_extract(projected.state_json, '$.pollStatus.status') = 'success'
      )
      AND EXISTS (
        SELECT 1 FROM main.agent_control_events github
        WHERE github.event_id = NEW.github_event_id
          AND github.aggregate_kind = 'github-intake'
          AND github.stream_id = NEW.project_id
          AND github.event_type = 'agentControl.github.poll.succeeded'
          AND github.sequence = NEW.github_event_sequence
          AND github.stream_version = NEW.github_event_stream_version
          AND github.sequence = NEW.github_intake_sequence
          AND ${sql.literal(SOURCE_FINGERPRINT_MATCH)}(
            NEW.source_fingerprint, NEW.project_id, NEW.github_intake_sequence,
            NEW.github_event_stream_version, NEW.github_event_stream_version,
            json_extract(github.payload_json, '$.repository.repositoryNodeId'),
            json_array_length(json_extract(github.payload_json, '$.issues'))
          ) = 1
      )
      AND EXISTS (
        SELECT 1 FROM main.agent_control_task_reconcile_states reconcile
        WHERE reconcile.project_id = NEW.project_id AND reconcile.status = 'completed'
          AND reconcile.target_sequence = NEW.github_intake_sequence
          AND reconcile.last_completed_sequence = NEW.github_intake_sequence
          AND reconcile.revision = NEW.reconcile_revision
      )
      AND NEW.task_frontier_sequence = COALESCE((
        SELECT MAX(task.last_event_sequence) FROM main.agent_control_task_states task
        WHERE task.project_id = NEW.project_id
      ), 0)
      AND NEW.task_frontier_revision = COALESCE((
        SELECT MAX(task.revision) FROM main.agent_control_task_states task
        WHERE task.project_id = NEW.project_id
      ), 0)
      AND NEW.task_frontier_count = (
        SELECT COUNT(*) FROM main.agent_control_task_states task
        WHERE task.project_id = NEW.project_id
      )
      AND NOT EXISTS (
        SELECT 1 FROM main.agent_control_task_states task
        WHERE task.project_id = NEW.project_id
          AND NOT EXISTS (
            SELECT 1 FROM json_each(CAST(NEW.payload_json AS TEXT), '$.taskFrontier') item
            WHERE json_extract(item.value, '$.taskId') = task.task_id
              AND json_extract(item.value, '$.issueNumber') = task.issue_number
              AND json_extract(item.value, '$.status') = task.status
              AND json_extract(item.value, '$.sourceGate') = task.source_gate
              AND json_extract(item.value, '$.stage') = task.stage
              AND json_extract(item.value, '$.githubIntakeSequence') = task.github_intake_sequence
              AND json_extract(item.value, '$.revision') = task.revision
              AND json_extract(item.value, '$.lastEventSequence') = task.last_event_sequence
          )
      )
      AND NOT EXISTS (
        SELECT 1 FROM json_each(CAST(NEW.payload_json AS TEXT), '$.taskFrontier') item
        WHERE NOT EXISTS (
          SELECT 1 FROM main.agent_control_task_states task
          WHERE task.project_id = NEW.project_id
            AND task.task_id = json_extract(item.value, '$.taskId')
            AND task.issue_number = json_extract(item.value, '$.issueNumber')
            AND task.status = json_extract(item.value, '$.status')
            AND task.source_gate = json_extract(item.value, '$.sourceGate')
            AND task.stage = json_extract(item.value, '$.stage')
            AND task.github_intake_sequence = json_extract(item.value, '$.githubIntakeSequence')
            AND task.revision = json_extract(item.value, '$.revision')
            AND task.last_event_sequence = json_extract(item.value, '$.lastEventSequence')
        )
      )
      AND EXISTS (
        SELECT 1 FROM main.agent_control_task_states selected
        WHERE selected.task_id = NEW.selected_task_id
          AND selected.project_id = NEW.project_id
          AND selected.github_intake_sequence = NEW.github_intake_sequence
          AND selected.status = 'candidate' AND selected.source_gate = 'eligible'
          AND selected.stage = 'intake'
          AND NOT EXISTS (
            SELECT 1 FROM main.agent_control_task_states earlier
            WHERE earlier.project_id = NEW.project_id
              AND earlier.github_intake_sequence = NEW.github_intake_sequence
              AND earlier.status = 'candidate' AND earlier.source_gate = 'eligible'
              AND earlier.stage = 'intake'
              AND (earlier.issue_number < selected.issue_number
                OR (earlier.issue_number = selected.issue_number
                  AND earlier.task_id < selected.task_id))
          )
      )
      AND NOT EXISTS (
        SELECT 1 FROM main.agent_control_armed_dispatch_states active
        WHERE active.project_id = NEW.project_id AND active.status IN ('claimed', 'activated')
      )
      AND NOT EXISTS (
        SELECT 1 FROM main.agent_control_run_once_states run
        WHERE run.project_id = NEW.project_id AND run.status = 'active'
      )
    )
    BEGIN SELECT RAISE(ABORT, 'armed dispatch evidence is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_armed_dispatch_receipt_validate
    BEFORE INSERT ON agent_control_armed_dispatch_receipts
    WHEN NOT EXISTS (
      SELECT 1 FROM main.agent_control_armed_dispatch_evidence evidence
      WHERE evidence.evidence_id = NEW.evidence_id AND evidence.receipt_id = NEW.receipt_id
        AND evidence.marker_id = NEW.marker_id AND evidence.dispatch_id = NEW.dispatch_id
        AND evidence.claim_id = NEW.claim_id AND NEW.status = 'accepted'
        AND evidence.claimed_at = NEW.accepted_at
    )
    BEGIN SELECT RAISE(ABORT, 'armed dispatch receipt is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_armed_dispatch_marker_validate
    BEFORE INSERT ON agent_control_armed_dispatch_markers
    WHEN NOT EXISTS (
      SELECT 1 FROM main.agent_control_armed_dispatch_evidence evidence
      JOIN main.agent_control_armed_dispatch_receipts receipt
        ON receipt.evidence_id = evidence.evidence_id AND receipt.receipt_id = evidence.receipt_id
      WHERE evidence.marker_id = NEW.marker_id AND evidence.evidence_id = NEW.evidence_id
        AND evidence.receipt_id = NEW.receipt_id AND evidence.dispatch_id = NEW.dispatch_id
        AND evidence.claim_id = NEW.claim_id AND receipt.marker_id = NEW.marker_id
        AND receipt.dispatch_id = NEW.dispatch_id AND receipt.claim_id = NEW.claim_id
        AND evidence.claimed_at = NEW.committed_at AND receipt.accepted_at = NEW.committed_at
        AND NEW.marker_fingerprint = evidence.payload_fingerprint
    )
    BEGIN SELECT RAISE(ABORT, 'armed dispatch marker is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_armed_dispatch_state_insert_validate
    BEFORE INSERT ON agent_control_armed_dispatch_states
    WHEN NOT EXISTS (
      SELECT 1 FROM main.agent_control_armed_dispatch_evidence evidence
      JOIN main.agent_control_armed_dispatch_receipts receipt
        ON receipt.evidence_id = evidence.evidence_id AND receipt.receipt_id = evidence.receipt_id
      WHERE evidence.dispatch_id = NEW.dispatch_id AND evidence.project_id = NEW.project_id
        AND NEW.status = 'claimed' AND NEW.owner_id = evidence.owner_id
        AND NEW.fence_token = evidence.fence_token AND NEW.expires_at = evidence.expires_at
        AND NEW.activation_event_id IS NULL AND NEW.activation_event_sequence IS NULL
        AND NEW.activation_event_stream_version IS NULL AND NEW.updated_at = evidence.claimed_at
    )
    BEGIN SELECT RAISE(ABORT, 'armed dispatch state is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_armed_dispatch_state_update_validate
    BEFORE UPDATE ON agent_control_armed_dispatch_states
    WHEN NOT (
      NEW.dispatch_id IS OLD.dispatch_id AND NEW.project_id IS OLD.project_id
      AND (
        (OLD.status = 'claimed' AND NEW.status = 'activated'
          AND NEW.owner_id IS OLD.owner_id AND NEW.fence_token = OLD.fence_token
          AND NEW.expires_at IS OLD.expires_at
          AND EXISTS (
            SELECT 1 FROM main.agent_control_armed_dispatch_evidence evidence
            JOIN main.agent_control_armed_dispatch_markers marker
              ON marker.marker_id = evidence.marker_id
            JOIN main.agent_control_events event ON event.event_id = NEW.activation_event_id
            JOIN main.agent_control_command_receipts receipt ON receipt.command_id = event.command_id
            WHERE evidence.dispatch_id = NEW.dispatch_id
              AND evidence.owner_id = OLD.owner_id
              AND evidence.fence_token = OLD.fence_token
              AND evidence.expires_at = OLD.expires_at
              AND event.command_id = evidence.mode_command_id
              AND event.stream_id = NEW.project_id
              AND event.sequence = NEW.activation_event_sequence
              AND event.stream_version = NEW.activation_event_stream_version
              AND event.actor_authority = 'system'
              AND receipt.authority = 'system' AND receipt.status = 'accepted'
              AND receipt.event_created = 1 AND receipt.result_sequence = event.sequence
              AND receipt.result_stream_version = event.stream_version
              AND event.occurred_at < OLD.expires_at
          ))
        OR
        (OLD.status = 'claimed' AND NEW.status = 'superseded'
          AND NEW.owner_id IS OLD.owner_id AND NEW.fence_token = OLD.fence_token
          AND NEW.expires_at IS OLD.expires_at
          AND NEW.activation_event_id IS NULL AND NEW.activation_event_sequence IS NULL
          AND NEW.activation_event_stream_version IS NULL
          AND OLD.expires_at <= NEW.updated_at)
        OR
        (OLD.status IN ('claimed', 'activated') AND NEW.status = 'superseded'
          AND NEW.owner_id IS OLD.owner_id AND NEW.fence_token = OLD.fence_token
          AND NEW.expires_at IS OLD.expires_at
          AND NEW.activation_event_id IS OLD.activation_event_id
          AND NEW.activation_event_sequence IS OLD.activation_event_sequence
          AND NEW.activation_event_stream_version IS OLD.activation_event_stream_version
          AND EXISTS (
            SELECT 1 FROM main.agent_control_project_states project
            WHERE project.project_id = NEW.project_id
              AND NOT (project.mode = 'armed' AND project.paused_from_mode IS NULL)
              AND NOT (project.mode = 'run-once' AND project.paused_from_mode IS NULL)
          ))
        OR
        (OLD.status = 'activated' AND NEW.status = 'completed'
          AND NEW.owner_id IS OLD.owner_id AND NEW.fence_token = OLD.fence_token
          AND NEW.expires_at IS OLD.expires_at
          AND NEW.activation_event_id IS OLD.activation_event_id
          AND NEW.activation_event_sequence IS OLD.activation_event_sequence
          AND NEW.activation_event_stream_version IS OLD.activation_event_stream_version
          AND EXISTS (
            SELECT 1 FROM main.agent_control_run_once_activations activation
            JOIN main.agent_control_run_once_states run ON run.run_id = activation.run_id
            JOIN main.agent_control_project_states project ON project.project_id = activation.project_id
            WHERE activation.armed_dispatch_id = NEW.dispatch_id
              AND activation.origin_mode = 'armed' AND run.status != 'active'
              AND project.mode = 'armed' AND project.paused_from_mode IS NULL
          ))
      )
    )
    BEGIN SELECT RAISE(ABORT, 'armed dispatch state transition is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_armed_dispatch_state_no_delete
    BEFORE DELETE ON agent_control_armed_dispatch_states
    BEGIN SELECT RAISE(ABORT, 'armed dispatch state is immutable'); END
  `;

  yield* sql`
    CREATE TRIGGER main.agent_control_armed_no_candidate_evidence_validate
    BEFORE INSERT ON agent_control_armed_no_candidate_evidence
    WHEN NOT (
      ${sql.literal(CANONICAL_BLOB_MATCH)}(NEW.payload_json, NEW.payload_fingerprint) = 1
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.schemaVersion') = 1
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.kind') = 'no-candidate'
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.evidenceId') = NEW.evidence_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.receiptId') = NEW.receipt_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.markerId') = NEW.marker_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.projectId') = NEW.project_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.projectRevision') = NEW.project_revision
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.projectEventSequence') = NEW.project_event_sequence
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.githubIntakeSequence') = NEW.github_intake_sequence
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.githubEventId') = NEW.github_event_id
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.githubEventSequence') = NEW.github_event_sequence
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.githubEventStreamVersion') = NEW.github_event_stream_version
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.sourceFingerprint') = NEW.source_fingerprint
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.reconcileRevision') = NEW.reconcile_revision
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.taskFrontierSequence') = NEW.task_frontier_sequence
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.taskFrontierRevision') = NEW.task_frontier_revision
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.taskFrontierCount') = NEW.task_frontier_count
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.epoch.taskFrontierFingerprint') = NEW.task_frontier_fingerprint
      AND json_extract(CAST(NEW.payload_json AS TEXT), '$.decidedAt') = NEW.decided_at
      AND json_type(CAST(NEW.payload_json AS TEXT), '$.taskFrontier') = 'array'
      AND json_array_length(json_extract(CAST(NEW.payload_json AS TEXT), '$.taskFrontier')) = NEW.task_frontier_count
      AND EXISTS (
        SELECT 1 FROM main.agent_control_github_intake_states projected
        WHERE projected.project_id = NEW.project_id
          AND projected.last_event_sequence = NEW.github_intake_sequence
          AND projected.revision = NEW.github_event_stream_version
          AND json_extract(projected.state_json, '$.config.revision') = NEW.github_event_stream_version
          AND json_extract(projected.state_json, '$.pollStatus.status') = 'success'
      )
      AND EXISTS (
        SELECT 1 FROM main.agent_control_project_states project
        WHERE project.project_id = NEW.project_id AND project.mode = 'armed'
          AND project.paused_from_mode IS NULL AND project.revision = NEW.project_revision
          AND project.last_event_sequence = NEW.project_event_sequence
      )
      AND EXISTS (
        SELECT 1 FROM main.agent_control_events github
        WHERE github.event_id = NEW.github_event_id AND github.aggregate_kind = 'github-intake'
          AND github.stream_id = NEW.project_id
          AND github.event_type = 'agentControl.github.poll.succeeded'
          AND github.sequence = NEW.github_event_sequence
          AND github.stream_version = NEW.github_event_stream_version
          AND github.sequence = NEW.github_intake_sequence
          AND ${sql.literal(SOURCE_FINGERPRINT_MATCH)}(
            NEW.source_fingerprint, NEW.project_id, NEW.github_intake_sequence,
            NEW.github_event_stream_version, NEW.github_event_stream_version,
            json_extract(github.payload_json, '$.repository.repositoryNodeId'),
            json_array_length(json_extract(github.payload_json, '$.issues'))
          ) = 1
      )
      AND EXISTS (
        SELECT 1 FROM main.agent_control_task_reconcile_states reconcile
        WHERE reconcile.project_id = NEW.project_id AND reconcile.status = 'completed'
          AND reconcile.target_sequence = NEW.github_intake_sequence
          AND reconcile.last_completed_sequence = NEW.github_intake_sequence
          AND reconcile.revision = NEW.reconcile_revision
      )
      AND NEW.task_frontier_sequence = COALESCE((
        SELECT MAX(task.last_event_sequence) FROM main.agent_control_task_states task
        WHERE task.project_id = NEW.project_id
      ), 0)
      AND NEW.task_frontier_revision = COALESCE((
        SELECT MAX(task.revision) FROM main.agent_control_task_states task
        WHERE task.project_id = NEW.project_id
      ), 0)
      AND NEW.task_frontier_count = (
        SELECT COUNT(*) FROM main.agent_control_task_states task WHERE task.project_id = NEW.project_id
      )
      AND NOT EXISTS (
        SELECT 1 FROM main.agent_control_task_states task
        WHERE task.project_id = NEW.project_id
          AND NOT EXISTS (
            SELECT 1 FROM json_each(CAST(NEW.payload_json AS TEXT), '$.taskFrontier') item
            WHERE json_extract(item.value, '$.taskId') = task.task_id
              AND json_extract(item.value, '$.issueNumber') = task.issue_number
              AND json_extract(item.value, '$.status') = task.status
              AND json_extract(item.value, '$.sourceGate') = task.source_gate
              AND json_extract(item.value, '$.stage') = task.stage
              AND json_extract(item.value, '$.githubIntakeSequence') = task.github_intake_sequence
              AND json_extract(item.value, '$.revision') = task.revision
              AND json_extract(item.value, '$.lastEventSequence') = task.last_event_sequence
          )
      )
      AND NOT EXISTS (
        SELECT 1 FROM json_each(CAST(NEW.payload_json AS TEXT), '$.taskFrontier') item
        WHERE NOT EXISTS (
          SELECT 1 FROM main.agent_control_task_states task
          WHERE task.project_id = NEW.project_id
            AND task.task_id = json_extract(item.value, '$.taskId')
            AND task.issue_number = json_extract(item.value, '$.issueNumber')
            AND task.status = json_extract(item.value, '$.status')
            AND task.source_gate = json_extract(item.value, '$.sourceGate')
            AND task.stage = json_extract(item.value, '$.stage')
            AND task.github_intake_sequence = json_extract(item.value, '$.githubIntakeSequence')
            AND task.revision = json_extract(item.value, '$.revision')
            AND task.last_event_sequence = json_extract(item.value, '$.lastEventSequence')
        )
      )
      AND NOT EXISTS (
        SELECT 1 FROM main.agent_control_task_states candidate
        WHERE candidate.project_id = NEW.project_id
          AND candidate.github_intake_sequence = NEW.github_intake_sequence
          AND candidate.status = 'candidate' AND candidate.source_gate = 'eligible'
          AND candidate.stage = 'intake'
      )
      AND NOT EXISTS (
        SELECT 1 FROM main.agent_control_armed_dispatch_states active
        WHERE active.project_id = NEW.project_id AND active.status IN ('claimed', 'activated')
      )
      AND NOT EXISTS (
        SELECT 1 FROM main.agent_control_run_once_states run
        WHERE run.project_id = NEW.project_id AND run.status = 'active'
      )
    )
    BEGIN SELECT RAISE(ABORT, 'armed no-candidate evidence is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_armed_no_candidate_receipt_validate
    BEFORE INSERT ON agent_control_armed_no_candidate_receipts
    WHEN NOT EXISTS (
      SELECT 1 FROM main.agent_control_armed_no_candidate_evidence evidence
      WHERE evidence.evidence_id = NEW.evidence_id AND evidence.receipt_id = NEW.receipt_id
        AND evidence.marker_id = NEW.marker_id AND NEW.status = 'accepted'
        AND evidence.decided_at = NEW.accepted_at
    )
    BEGIN SELECT RAISE(ABORT, 'armed no-candidate receipt is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_armed_no_candidate_marker_validate
    BEFORE INSERT ON agent_control_armed_no_candidate_markers
    WHEN NOT EXISTS (
      SELECT 1 FROM main.agent_control_armed_no_candidate_evidence evidence
      JOIN main.agent_control_armed_no_candidate_receipts receipt
        ON receipt.evidence_id = evidence.evidence_id AND receipt.receipt_id = evidence.receipt_id
      WHERE evidence.marker_id = NEW.marker_id AND evidence.evidence_id = NEW.evidence_id
        AND evidence.receipt_id = NEW.receipt_id AND receipt.marker_id = NEW.marker_id
        AND evidence.decided_at = NEW.committed_at AND receipt.accepted_at = NEW.committed_at
        AND NEW.marker_fingerprint = evidence.payload_fingerprint
    )
    BEGIN SELECT RAISE(ABORT, 'armed no-candidate marker is inconsistent'); END
  `;
  for (const table of [
    "agent_control_armed_dispatch_evidence",
    "agent_control_armed_dispatch_receipts",
    "agent_control_armed_dispatch_markers",
    "agent_control_armed_no_candidate_evidence",
    "agent_control_armed_no_candidate_receipts",
    "agent_control_armed_no_candidate_markers",
  ]) {
    yield* sql.unsafe(`
      CREATE TRIGGER main.${quote(`${table}_no_update`)}
      BEFORE UPDATE ON ${quote(table)}
      BEGIN SELECT RAISE(ABORT, 'armed authority is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.${quote(`${table}_no_delete`)}
      BEFORE DELETE ON ${quote(table)}
      BEGIN SELECT RAISE(ABORT, 'armed authority is immutable'); END
    `).unprepared;
  }
});

const replaceRunOnceTriggers = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const legacyEvidenceTriggers = yield* sql<SchemaObject>`
    SELECT name, sql FROM main.sqlite_schema
    WHERE type = 'trigger' AND name = 'agent_control_run_once_evidence_validate'
      AND sql IS NOT NULL
  `;
  const legacyEvidenceTrigger = legacyEvidenceTriggers[0];
  const legacyActiveSteps = "NEW.step IN ('mode-reset', 'mode-reset-superseded', 'completed')";
  if (
    legacyEvidenceTriggers.length !== 1 ||
    legacyEvidenceTrigger === undefined ||
    legacyEvidenceTrigger.sql.split(legacyActiveSteps).length !== 2
  ) {
    return yield* Effect.die(
      new Error("migration 064 encountered divergent run-once evidence authority"),
    );
  }
  const armedEvidenceTrigger: SchemaObject = {
    ...legacyEvidenceTrigger,
    sql: legacyEvidenceTrigger.sql.replace(
      legacyActiveSteps,
      "NEW.step IN ('activation-admitted', 'mode-reset', 'mode-reset-superseded', 'completed')",
    ),
  };
  for (const name of [
    "agent_control_run_once_activation_event_validate",
    "agent_control_run_once_activation_project_validate",
    "agent_control_run_once_evidence_validate",
    "agent_control_run_once_mode_evidence_validate",
  ]) {
    yield* sql.unsafe(`DROP TRIGGER main.${quote(name)}`).unprepared;
  }

  yield* sql`
    CREATE TRIGGER main.agent_control_armed_system_activation_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'project-controller'
      AND NEW.event_type = 'agentControl.project.mode.changed'
      AND NEW.actor_authority = 'system'
      AND json_extract(NEW.payload_json, '$.previousMode') = 'armed'
      AND json_extract(NEW.payload_json, '$.mode') = 'run-once'
    BEGIN
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM main.agent_control_armed_dispatch_evidence evidence
        JOIN main.agent_control_armed_dispatch_receipts receipt
          ON receipt.evidence_id = evidence.evidence_id AND receipt.receipt_id = evidence.receipt_id
        JOIN main.agent_control_armed_dispatch_markers marker
          ON marker.evidence_id = evidence.evidence_id AND marker.receipt_id = receipt.receipt_id
        JOIN main.agent_control_armed_dispatch_states state
          ON state.dispatch_id = evidence.dispatch_id
        JOIN main.agent_control_project_states project
          ON project.project_id = evidence.project_id
        WHERE evidence.mode_command_id = NEW.command_id
          AND evidence.project_id = NEW.stream_id
          AND receipt.status = 'accepted' AND state.status = 'claimed'
          AND state.owner_id = evidence.owner_id
          AND state.fence_token = evidence.fence_token
          AND state.expires_at = evidence.expires_at
          AND NEW.occurred_at < state.expires_at
          AND project.mode = 'armed' AND project.paused_from_mode IS NULL
          AND project.revision = evidence.project_revision
          AND project.last_event_sequence = evidence.project_event_sequence
          AND NEW.stream_version = project.revision + 1
          AND EXISTS (
            SELECT 1 FROM main.agent_control_github_intake_states projected
            WHERE projected.project_id = evidence.project_id
              AND projected.last_event_sequence = evidence.github_intake_sequence
              AND projected.revision = evidence.github_event_stream_version
              AND json_extract(projected.state_json, '$.config.revision') =
                evidence.github_event_stream_version
              AND json_extract(projected.state_json, '$.pollStatus.status') = 'success'
          )
          AND EXISTS (
            SELECT 1 FROM main.agent_control_events github
            WHERE github.event_id = evidence.github_event_id
              AND github.aggregate_kind = 'github-intake'
              AND github.stream_id = evidence.project_id
              AND github.event_type = 'agentControl.github.poll.succeeded'
              AND github.sequence = evidence.github_event_sequence
              AND github.stream_version = evidence.github_event_stream_version
              AND github.sequence = evidence.github_intake_sequence
              AND ${sql.literal(SOURCE_FINGERPRINT_MATCH)}(
                evidence.source_fingerprint, evidence.project_id,
                evidence.github_intake_sequence, evidence.github_event_stream_version,
                evidence.github_event_stream_version,
                json_extract(github.payload_json, '$.repository.repositoryNodeId'),
                json_array_length(json_extract(github.payload_json, '$.issues'))
              ) = 1
          )
          AND EXISTS (
            SELECT 1 FROM main.agent_control_task_reconcile_states reconcile
            WHERE reconcile.project_id = evidence.project_id
              AND reconcile.status = 'completed'
              AND reconcile.target_sequence = evidence.github_intake_sequence
              AND reconcile.last_completed_sequence = evidence.github_intake_sequence
              AND reconcile.revision = evidence.reconcile_revision
          )
          AND evidence.task_frontier_sequence = COALESCE((
            SELECT MAX(task.last_event_sequence)
            FROM main.agent_control_task_states task
            WHERE task.project_id = evidence.project_id
          ), 0)
          AND evidence.task_frontier_revision = COALESCE((
            SELECT MAX(task.revision)
            FROM main.agent_control_task_states task
            WHERE task.project_id = evidence.project_id
          ), 0)
          AND evidence.task_frontier_count = (
            SELECT COUNT(*) FROM main.agent_control_task_states task
            WHERE task.project_id = evidence.project_id
          )
          AND NOT EXISTS (
            SELECT 1 FROM main.agent_control_task_states task
            WHERE task.project_id = evidence.project_id
              AND NOT EXISTS (
                SELECT 1
                FROM json_each(CAST(evidence.payload_json AS TEXT), '$.taskFrontier') item
                WHERE json_extract(item.value, '$.taskId') = task.task_id
                  AND json_extract(item.value, '$.issueNumber') = task.issue_number
                  AND json_extract(item.value, '$.status') = task.status
                  AND json_extract(item.value, '$.sourceGate') = task.source_gate
                  AND json_extract(item.value, '$.stage') = task.stage
                  AND json_extract(item.value, '$.githubIntakeSequence') =
                    task.github_intake_sequence
                  AND json_extract(item.value, '$.revision') = task.revision
                  AND json_extract(item.value, '$.lastEventSequence') =
                    task.last_event_sequence
              )
          )
          AND NOT EXISTS (
            SELECT 1
            FROM json_each(CAST(evidence.payload_json AS TEXT), '$.taskFrontier') item
            WHERE NOT EXISTS (
              SELECT 1 FROM main.agent_control_task_states task
              WHERE task.project_id = evidence.project_id
                AND task.task_id = json_extract(item.value, '$.taskId')
                AND task.issue_number = json_extract(item.value, '$.issueNumber')
                AND task.status = json_extract(item.value, '$.status')
                AND task.source_gate = json_extract(item.value, '$.sourceGate')
                AND task.stage = json_extract(item.value, '$.stage')
                AND task.github_intake_sequence =
                  json_extract(item.value, '$.githubIntakeSequence')
                AND task.revision = json_extract(item.value, '$.revision')
                AND task.last_event_sequence =
                  json_extract(item.value, '$.lastEventSequence')
            )
          )
          AND EXISTS (
            SELECT 1 FROM main.agent_control_task_states selected
            WHERE selected.task_id = evidence.selected_task_id
              AND selected.project_id = evidence.project_id
              AND selected.github_intake_sequence = evidence.github_intake_sequence
              AND selected.status = 'candidate' AND selected.source_gate = 'eligible'
              AND selected.stage = 'intake'
              AND NOT EXISTS (
                SELECT 1 FROM main.agent_control_task_states earlier
                WHERE earlier.project_id = evidence.project_id
                  AND earlier.github_intake_sequence = evidence.github_intake_sequence
                  AND earlier.status = 'candidate' AND earlier.source_gate = 'eligible'
                  AND earlier.stage = 'intake'
                  AND (earlier.issue_number < selected.issue_number
                    OR (earlier.issue_number = selected.issue_number
                      AND earlier.task_id < selected.task_id))
              )
          )
          AND json_extract(NEW.payload_json, '$.projectId') = evidence.project_id
          AND json_extract(NEW.payload_json, '$.previousPausedFromMode') IS NULL
          AND json_extract(NEW.payload_json, '$.pausedFromMode') IS NULL
          AND NEW.correlation_id = NEW.command_id AND NEW.causation_event_id IS NULL
      ) THEN RAISE(ABORT, 'armed system activation authority is inconsistent') END;
    END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_system_reset_origin_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'project-controller'
      AND NEW.event_type = 'agentControl.project.mode.changed'
      AND NEW.actor_authority = 'system'
      AND json_extract(NEW.payload_json, '$.previousMode') = 'run-once'
      AND json_extract(NEW.payload_json, '$.mode') IN ('observe', 'armed')
    BEGIN
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM main.agent_control_run_once_activations activation
        JOIN main.agent_control_run_once_states state ON state.run_id = activation.run_id
        WHERE activation.project_id = NEW.stream_id AND state.status = 'active'
          AND state.last_step IN ('no-eligible-task', 'task-terminal-observed')
          AND activation.origin_mode = json_extract(NEW.payload_json, '$.mode')
          AND ${sql.literal(RUN_ONCE_STEP_IDENTITY_MATCH)}(
            'command', NEW.command_id, activation.run_id, state.next_ordinal, 'mode-reset'
          ) = 1
          AND json_extract(NEW.payload_json, '$.projectId') = activation.project_id
          AND json_extract(NEW.payload_json, '$.previousPausedFromMode') IS NULL
          AND json_extract(NEW.payload_json, '$.pausedFromMode') IS NULL
          AND NEW.correlation_id = NEW.command_id AND NEW.causation_event_id IS NULL
      ) THEN RAISE(ABORT, 'run-once system reset origin is inconsistent') END;
    END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_activation_event_validate
    BEFORE INSERT ON agent_control_run_once_activations
    WHEN NOT EXISTS (
      SELECT 1
      FROM main.agent_control_events activation
      JOIN main.agent_control_command_receipts receipt ON receipt.command_id = activation.command_id
      WHERE activation.event_id = NEW.activation_event_id
        AND activation.aggregate_kind = 'project-controller'
        AND activation.stream_id = NEW.project_id
        AND activation.event_type = 'agentControl.project.mode.changed'
        AND activation.sequence = NEW.activation_event_sequence
        AND activation.stream_version = NEW.activation_event_stream_version
        AND activation.command_id = NEW.activation_command_id
        AND activation.correlation_id = NEW.activation_command_id
        AND activation.causation_event_id IS NULL
        AND activation.occurred_at = NEW.activated_at
        AND typeof(activation.payload_json) = 'text'
        AND typeof(activation.metadata_json) = 'text'
        AND NEW.activation_event_payload_json = CAST(activation.payload_json AS BLOB)
        AND NEW.activation_event_metadata_json = CAST(activation.metadata_json AS BLOB)
        AND ${sql.literal(MODE_EVENT_MATCH)}(
          NEW.activation_event_payload_json, NEW.activation_event_metadata_json,
          NEW.project_id, NEW.origin_mode, 'run-once', NULL, NULL, NEW.activated_at
        ) = 1
        AND activation.actor_authority = CASE NEW.origin_mode WHEN 'observe' THEN 'human' ELSE 'system' END
        AND receipt.authority = activation.actor_authority
        AND receipt.aggregate_kind = 'project-controller'
        AND receipt.aggregate_id = NEW.project_id AND receipt.status = 'accepted'
        AND receipt.event_created = 1 AND receipt.result_sequence = activation.sequence
        AND receipt.result_stream_version = activation.stream_version
        AND receipt.accepted_at = activation.occurred_at AND receipt.error_code IS NULL
        AND receipt.command_fingerprint = NEW.activation_command_fingerprint
        AND ${sql.literal(MODE_COMMAND_FINGERPRINT_MATCH)}(
          receipt.command_fingerprint, receipt.command_id, NEW.project_id,
          NEW.activation_expected_revision, 'run-once'
        ) = 1
        AND (
          (NEW.origin_mode = 'observe' AND NEW.armed_dispatch_id IS NULL
            AND NEW.armed_claim_id IS NULL AND NEW.armed_marker_id IS NULL)
          OR
          (NEW.origin_mode = 'armed' AND EXISTS (
            SELECT 1 FROM main.agent_control_armed_dispatch_evidence evidence
            JOIN main.agent_control_armed_dispatch_receipts armed_receipt
              ON armed_receipt.evidence_id = evidence.evidence_id
            JOIN main.agent_control_armed_dispatch_markers marker
              ON marker.evidence_id = evidence.evidence_id
            JOIN main.agent_control_armed_dispatch_states state
              ON state.dispatch_id = evidence.dispatch_id
            WHERE evidence.dispatch_id = NEW.armed_dispatch_id
              AND evidence.claim_id = NEW.armed_claim_id
              AND evidence.marker_id = NEW.armed_marker_id
              AND evidence.mode_command_id = NEW.activation_command_id
              AND evidence.project_id = NEW.project_id
              AND evidence.github_intake_sequence = NEW.github_intake_sequence
              AND evidence.github_event_id = NEW.github_event_id
              AND evidence.github_event_sequence = NEW.github_event_sequence
              AND evidence.github_event_stream_version = NEW.github_event_stream_version
              AND evidence.reconcile_revision = NEW.reconcile_revision
              AND evidence.source_fingerprint = NEW.source_fingerprint
              AND state.activation_event_id = NEW.activation_event_id
              AND state.activation_event_sequence = NEW.activation_event_sequence
              AND state.activation_event_stream_version = NEW.activation_event_stream_version
              AND state.status = 'activated'
          ))
        )
    )
    BEGIN SELECT RAISE(ABORT, 'run-once activation event authority is inconsistent'); END
  `;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_activation_project_validate
    BEFORE INSERT ON agent_control_run_once_activations
    WHEN NOT EXISTS (
      WITH RECURSIVE lineage(revision, mode, paused_from_mode, valid) AS (
        SELECT NEW.activation_event_stream_version, 'run-once', NULL, 1
        UNION ALL
        SELECT event.stream_version,
          json_extract(event.payload_json, '$.mode'),
          json_extract(event.payload_json, '$.pausedFromMode'),
          CASE WHEN lineage.valid = 1
            AND event.actor_authority = 'human'
            AND event.event_type = 'agentControl.project.mode.changed'
            AND event.correlation_id = event.command_id AND event.causation_event_id IS NULL
            AND json_extract(event.payload_json, '$.projectId') = NEW.project_id
            AND json_extract(event.payload_json, '$.previousMode') = lineage.mode
            AND json_extract(event.payload_json, '$.previousPausedFromMode') IS lineage.paused_from_mode
            AND (
              (lineage.mode = 'run-once'
                AND json_extract(event.payload_json, '$.mode') IN ('manual', 'observe')
                AND json_extract(event.payload_json, '$.pausedFromMode') IS NULL)
              OR (lineage.mode = 'run-once'
                AND json_extract(event.payload_json, '$.mode') = 'paused'
                AND json_extract(event.payload_json, '$.pausedFromMode') = 'run-once')
              OR (lineage.mode = 'paused' AND lineage.paused_from_mode = 'run-once'
                AND json_extract(event.payload_json, '$.mode') IN ('run-once', 'manual')
                AND json_extract(event.payload_json, '$.pausedFromMode') IS NULL)
            )
          THEN 1 ELSE 0 END
        FROM lineage
        JOIN main.agent_control_events event
          ON event.aggregate_kind = 'project-controller' AND event.stream_id = NEW.project_id
         AND event.stream_version = lineage.revision + 1
      )
      SELECT 1 FROM lineage
      JOIN main.agent_control_project_states project
        ON project.project_id = NEW.project_id AND project.revision = lineage.revision
      WHERE lineage.valid = 1 AND project.mode = lineage.mode
        AND project.paused_from_mode IS lineage.paused_from_mode
        AND project.last_event_sequence = (
          SELECT event.sequence FROM main.agent_control_events event
          WHERE event.aggregate_kind = 'project-controller' AND event.stream_id = NEW.project_id
            AND event.stream_version = lineage.revision
        )
    )
    BEGIN SELECT RAISE(ABORT, 'run-once activation project authority is inconsistent'); END
  `;
  yield* sql.unsafe(armedEvidenceTrigger.sql).unprepared;
  yield* sql`
    CREATE TRIGGER main.agent_control_run_once_mode_evidence_validate
    BEFORE INSERT ON agent_control_run_once_step_evidence
    WHEN NEW.step = 'mode-reset' AND NOT EXISTS (
      SELECT 1 FROM main.agent_control_events event
      JOIN main.agent_control_command_receipts receipt ON receipt.command_id = event.command_id
      JOIN main.agent_control_run_once_activations activation ON activation.run_id = NEW.run_id
      WHERE event.event_id = NEW.mode_event_id
        AND event.aggregate_kind = 'project-controller' AND event.stream_id = NEW.project_id
        AND event.event_type = 'agentControl.project.mode.changed'
        AND event.actor_authority = 'system' AND event.command_id = NEW.command_id
        AND event.correlation_id = NEW.command_id AND event.causation_event_id IS NULL
        AND event.sequence = NEW.mode_event_sequence
        AND event.stream_version = NEW.mode_event_stream_version
        AND typeof(event.payload_json) = 'text' AND typeof(event.metadata_json) = 'text'
        AND NEW.mode_event_payload_json = CAST(event.payload_json AS BLOB)
        AND NEW.mode_event_metadata_json = CAST(event.metadata_json AS BLOB)
        AND ${sql.literal(MODE_EVENT_MATCH)}(
          NEW.mode_event_payload_json, NEW.mode_event_metadata_json,
          NEW.project_id, 'run-once', activation.origin_mode, NULL, NULL, event.occurred_at
        ) = 1
        AND receipt.authority = 'system' AND receipt.aggregate_kind = 'project-controller'
        AND receipt.aggregate_id = NEW.project_id AND receipt.status = 'accepted'
        AND receipt.event_created = 1 AND receipt.result_sequence = event.sequence
        AND receipt.result_stream_version = event.stream_version
        AND receipt.accepted_at = event.occurred_at AND receipt.error_code IS NULL
        AND receipt.command_fingerprint = NEW.mode_command_fingerprint
        AND ${sql.literal(MODE_COMMAND_FINGERPRINT_MATCH)}(
          receipt.command_fingerprint, receipt.command_id, NEW.project_id,
          NEW.mode_expected_revision, activation.origin_mode
        ) = 1
    )
    BEGIN SELECT RAISE(ABORT, 'run-once mode reset authority is inconsistent'); END
  `;
});

export const makeMigration064 = (
  injectFault: (point: Migration064FaultPoint) => Effect.Effect<void> = () => Effect.void,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const collisions = yield* sql<{ readonly name: string }>`
      SELECT name FROM main.sqlite_schema
      WHERE name LIKE 'agent_control_armed_%'
         OR name IN (
           'idx_agent_control_run_once_armed_dispatch',
           'idx_agent_control_armed_task_frontier',
           'idx_agent_control_armed_project_catchup',
           'agent_control_run_once_system_reset_origin_validate'
         )
      ORDER BY name
    `;
    const activationColumns = yield* sql<{ readonly name: string }>`
      SELECT name FROM pragma_table_xinfo('agent_control_run_once_activations')
      WHERE name IN ('armed_dispatch_id', 'armed_claim_id', 'armed_marker_id', 'origin_mode')
      ORDER BY name
    `;
    const required063 = yield* sql<SchemaObject>`
      SELECT name, sql FROM main.sqlite_schema WHERE sql IS NOT NULL AND name IN (
        'idx_agent_control_project_states_sequence',
        'agent_control_project_states', 'agent_control_run_once_activations',
        'agent_control_run_once_activation_event_validate',
        'agent_control_run_once_activation_project_validate',
        'agent_control_run_once_active_lineage_validate',
        'agent_control_run_once_evidence_validate',
        'agent_control_run_once_mode_evidence_validate',
        'idx_agent_control_run_once_candidates'
      ) ORDER BY name
    `;
    if (
      collisions.length !== 0 ||
      activationColumns.length !== 0 ||
      required063.length !== EXPECTED_063_DDL_FINGERPRINTS.size ||
      required063.some(
        (object) => EXPECTED_063_DDL_FINGERPRINTS.get(object.name) !== sha256Utf8(object.sql),
      )
    ) {
      return yield* Effect.die(new Error("migration 064 encountered partial or divergent schema"));
    }
    const udf = yield* sql<{
      readonly canonical: number;
      readonly source: number;
      readonly command: number;
      readonly event: number;
      readonly step: number;
    }>`
      SELECT
        ${sql.literal(CANONICAL_BLOB_MATCH)}(
          CAST('{"schemaVersion":1}' AS BLOB),
          '0e9561cfb83d50990a103b3896fe249a11fe27fa28985448187f93ec12116d72'
        ) AS canonical,
        ${sql.literal(SOURCE_FINGERPRINT_MATCH)}(
          ${"0".repeat(64)}, 'invalid', 1, 1, 1, 'invalid', 0
        ) AS source,
        ${sql.literal(MODE_COMMAND_FINGERPRINT_MATCH)}(
          ${"0".repeat(64)}, 'invalid', 'invalid', 0, 'armed'
        ) AS command,
        ${sql.literal(MODE_EVENT_MATCH)}(
          CAST(${canonicalModeEventProbe} AS BLOB), CAST('{"schemaVersion":1}' AS BLOB),
          'invalid', 'armed', 'run-once', NULL, NULL, '2026-01-01T00:00:00.000Z'
        ) AS event,
        ${sql.literal(RUN_ONCE_STEP_IDENTITY_MATCH)}(
          'command', 'invalid', 'invalid', 1, 'mode-reset'
        ) AS step
    `;
    if (
      udf.length !== 1 ||
      udf[0]?.canonical !== 1 ||
      udf[0].source !== 0 ||
      udf[0].command !== 0 ||
      udf[0].event !== 1 ||
      udf[0].step !== 0
    ) {
      return yield* Effect.die(new Error("migration 064 requires run-once authority UDFs"));
    }
    yield* sql`PRAGMA defer_foreign_keys = ON`;
    yield* injectFault("before-project-state-rebuild");
    yield* rebuildProjectStates;
    yield* injectFault("after-project-state-rebuild");
    yield* extendRunOnceActivations;
    yield* createArmedTables;
    yield* injectFault("after-armed-tables");
    yield* createArmedTriggers;
    yield* replaceRunOnceTriggers;
    yield* injectFault("after-authority-triggers");
    const foreignKeys = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
    if (foreignKeys.length !== 0) {
      return yield* Effect.die(new Error("migration 064 introduced foreign-key violations"));
    }
    const integrity = yield* sql<{ readonly integrity_check: string }>`PRAGMA integrity_check`;
    if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") {
      return yield* Effect.die(new Error("migration 064 failed integrity_check"));
    }
    yield* sql`PRAGMA defer_foreign_keys = OFF`;
  });

export default makeMigration064();
