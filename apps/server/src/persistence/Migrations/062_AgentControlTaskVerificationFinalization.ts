import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  sha256Utf8,
  type JsonValue,
} from "../../agentControl/initialPlanning/eventEvidence.ts";

const TASK_FINALIZATION_PAYLOAD_STORAGE_FUNCTION =
  "t3_task_verification_finalization_payload_storage";
const TASK_FINALIZATION_DOCUMENT_STORAGE_FUNCTION =
  "t3_task_verification_finalization_document_storage";
const TASK_FINALIZATION_MARKER_MATCH_FUNCTION = "t3_task_verification_finalization_marker_match";
const TASK_FINALIZATION_PROJECTION_MATCH_FUNCTION =
  "t3_task_verification_finalization_projection_match";

const taskFinalizationIdentity = (prefix: string, domain: string, parts: ReadonlyArray<string>) =>
  `${prefix}-${sha256Utf8(
    canonicalJson({ domain: `agent-control-task-${domain}-v1`, parts } as unknown as JsonValue),
  )}`;
const udfPayloadBase = {
  projectId: "migration-062-udf-project",
  taskId: "migration-062-udf-task",
  verificationTaskRevision: 1,
  previousTaskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: sha256Utf8("migration-062-udf-source"),
  taskSourceEventId: "migration-062-udf-source-event",
  taskSourceEventSequence: 1,
  taskSourceEventStreamVersion: 1,
  handoffId: "migration-062-udf-handoff",
  handoffFingerprint: sha256Utf8("migration-062-udf-handoff"),
  verificationFinalizationEvidenceId: "migration-062-udf-verification-evidence",
  verificationFinalizationReceiptId: "migration-062-udf-verification-receipt",
  verificationFinalizationMarkerId: "migration-062-udf-verification-marker",
  verificationFinalizationCommandId: "migration-062-udf-verification-command",
  verificationFinalizationFingerprint: sha256Utf8("migration-062-udf-verification-evidence"),
  verificationFinalizationMarkerFingerprint: sha256Utf8("migration-062-udf-verification-marker"),
  terminalStageRunId: "migration-062-udf-stage-run",
  terminalStageEventId: "migration-062-udf-stage-event",
  terminalStageEventSequence: 2,
  terminalStageEventStreamVersion: 3,
  releasedLeaseId: "migration-062-udf-lease",
  releasedLeaseEventId: "migration-062-udf-lease-event",
  releasedLeaseEventSequence: 3,
  releasedLeaseEventStreamVersion: 8,
  terminalRuntimeEventId: "migration-062-udf-runtime-event",
  deliveryTerminalState: "completed",
  verificationOutcome: "succeeded",
  terminalCause: "verification-passed",
  previousStatus: "candidate",
  status: "succeeded",
  stage: "verification",
  evaluation: {
    evaluationAuthority: "accepted-evaluation",
    evaluationId: "migration-062-udf-evaluation",
    evaluationEvidenceId: "migration-062-udf-evaluation-evidence",
    evaluationReceiptId: "migration-062-udf-evaluation-receipt",
    evaluationMarkerId: "migration-062-udf-evaluation-marker",
    evaluationDisposition: "evaluated",
    verificationVerdict: "passed",
    invalidOutputCode: null,
  },
  finalizedAt: "2026-08-30T10:00:00.000Z",
} as const;
const udfIdentityParts = [
  udfPayloadBase.verificationFinalizationMarkerId,
  udfPayloadBase.taskId,
  String(udfPayloadBase.verificationTaskRevision),
] as const;
const udfIds = {
  commandId: taskFinalizationIdentity(
    "task-verification-finalization",
    "verification-finalization-command",
    udfIdentityParts,
  ),
  evidenceId: taskFinalizationIdentity(
    "task-verification-finalization-evidence",
    "verification-finalization-evidence",
    udfIdentityParts,
  ),
  receiptId: taskFinalizationIdentity(
    "task-verification-finalization-receipt",
    "verification-finalization-receipt",
    udfIdentityParts,
  ),
  markerId: taskFinalizationIdentity(
    "task-verification-finalization-marker",
    "verification-finalization-marker",
    udfIdentityParts,
  ),
  eventId: taskFinalizationIdentity(
    "task-finalized-after-verification-event",
    "finalized-after-verification-event",
    udfIdentityParts,
  ),
} as const;
const udfPayload = { ...udfPayloadBase, taskFinalizationEvidenceId: udfIds.evidenceId } as const;
const udfDocument = {
  schemaVersion: 1,
  commandId: udfIds.commandId,
  taskFinalizationEvidenceId: udfIds.evidenceId,
  taskFinalizationReceiptId: udfIds.receiptId,
  taskFinalizationMarkerId: udfIds.markerId,
  verificationFinalizationEvidenceId: udfPayload.verificationFinalizationEvidenceId,
  verificationFinalizationReceiptId: udfPayload.verificationFinalizationReceiptId,
  verificationFinalizationMarkerId: udfPayload.verificationFinalizationMarkerId,
  verificationFinalizationCommandId: udfPayload.verificationFinalizationCommandId,
  verificationFinalizationFingerprint: udfPayload.verificationFinalizationFingerprint,
  verificationFinalizationMarkerFingerprint: udfPayload.verificationFinalizationMarkerFingerprint,
  taskEventId: udfIds.eventId,
  taskEventStreamVersion: 2,
  payload: udfPayload,
  finalizedAt: udfPayload.finalizedAt,
} as const;
const udfPayloadJson = canonicalJson(udfPayload as unknown as JsonValue);
const udfDocumentJson = canonicalJson(udfDocument as unknown as JsonValue);
const udfFinalizationFingerprint = sha256Utf8(udfDocumentJson);
const udfMarkerFingerprint = sha256Utf8(
  canonicalJson({
    domain: "agent-control-task-verification-finalization-marker-v1",
    evidenceId: udfIds.evidenceId,
    receiptId: udfIds.receiptId,
    markerId: udfIds.markerId,
    commandId: udfIds.commandId,
    finalizationFingerprint: udfFinalizationFingerprint,
    verificationMarkerId: udfPayload.verificationFinalizationMarkerId,
    eventId: udfIds.eventId,
    finalizedAt: udfPayload.finalizedAt,
  } as unknown as JsonValue),
);
const udfOldTaskState = {
  schemaVersion: 1,
  taskId: udfPayload.taskId,
  source: {
    projectId: udfPayload.projectId,
    repositoryNodeId: "migration-062-udf-repository",
    issueNodeId: "migration-062-udf-issue",
    issueNumber: 1,
    issueUrl: "https://example.invalid/issues/1",
  },
  status: "candidate",
  sourceGate: "eligible",
  stage: "intake",
  sourceUpdatedAt: "2026-08-30T09:00:00.000Z",
  githubIntakeSequence: udfPayload.githubIntakeSequence,
  sourceSnapshot: {
    repositoryNodeId: "migration-062-udf-repository",
    issueNodeId: "migration-062-udf-issue",
    number: 1,
    url: "https://example.invalid/issues/1",
    state: "open",
    title: "Migration 062 UDF preflight",
    body: null,
    contentTrust: "untrusted-external",
    updatedAt: "2026-08-30T09:00:00.000Z",
    timelineComplete: true,
    ready: true,
    paused: false,
    eligible: true,
    eligibilityReason: "eligible",
  },
  createdAt: "2026-08-30T09:00:00.000Z",
  updatedAt: "2026-08-30T09:00:00.000Z",
  revision: 1,
  sequence: 1,
} as const;
const udfNewTaskState = {
  ...udfOldTaskState,
  status: udfPayload.status,
  stage: "verification",
  updatedAt: udfPayload.finalizedAt,
  revision: 2,
  sequence: 2,
} as const;
// JSON.stringify intentionally preserves a non-canonical key order here. Historical Task state
// bytes are valid typed authority and must not be rewritten merely to close the projection.
const udfOldTaskStateJson = JSON.stringify(udfOldTaskState);
const udfNewTaskStateJson = canonicalJson(udfNewTaskState as unknown as JsonValue);
const udfDivergentTaskStateJson = canonicalJson({
  ...udfNewTaskState,
  status: "failed",
} as unknown as JsonValue);

const quote = (identifier: string) => `"${identifier.replaceAll('"', '""')}"`;

interface SchemaObject {
  readonly name: string;
  readonly sql: string;
}

const restoreSchema = (objects: ReadonlyArray<SchemaObject>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const object of objects) yield* sql.unsafe(object.sql).unprepared;
  });

export type Migration062FaultPoint =
  | "before-events-rebuild"
  | "after-events-rebuild"
  | "after-task-states-rebuild"
  | "after-companions"
  | "after-install";

const rebuildAgentControlEvents = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [triggers, indexes, sequenceRows] = yield* Effect.all([
    sql<SchemaObject>`
      SELECT name, sql FROM main.sqlite_schema
      WHERE type = 'trigger' AND sql IS NOT NULL
        AND (tbl_name = 'agent_control_events' OR sql LIKE '%agent_control_events%')
      ORDER BY name
    `,
    sql<SchemaObject>`
      SELECT name, sql FROM main.sqlite_schema
      WHERE type = 'index' AND tbl_name = 'agent_control_events' AND sql IS NOT NULL
      ORDER BY name
    `,
    sql<{ readonly seq: number }>`
      SELECT seq FROM main.sqlite_sequence WHERE name = 'agent_control_events'
    `,
  ]);
  for (const trigger of triggers) {
    yield* sql.unsafe(`DROP TRIGGER main.${quote(trigger.name)}`).unprepared;
  }
  yield* sql`
    CREATE TABLE main.agent_control_events_rebuild_062 (
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
          'agentControl.task.needsAttentionMarked', 'agentControl.task.sourceMissingRecovered',
          'agentControl.task.finalizedAfterVerification'
        ))
        OR (aggregate_kind = 'stage-run' AND event_type IN (
          'agentControl.stageRun.prepared', 'agentControl.stageRun.planningStarted',
          'agentControl.stageRun.planningSucceeded', 'agentControl.stageRun.planningFailed',
          'agentControl.stageRun.planningCancelled', 'agentControl.stageRun.implementationStarted',
          'agentControl.stageRun.implementationSucceeded',
          'agentControl.stageRun.implementationFailed',
          'agentControl.stageRun.implementationCancelled',
          'agentControl.stageRun.verificationStarted',
          'agentControl.stageRun.verificationSucceeded',
          'agentControl.stageRun.verificationFailed',
          'agentControl.stageRun.verificationCancelled'
        ))
        OR (aggregate_kind = 'stage-run-lease' AND event_type IN (
          'agentControl.stageRunLease.reserved', 'agentControl.stageRunLease.renewed',
          'agentControl.stageRunLease.releasedBeforeExecution',
          'agentControl.stageRunLease.releasedAfterPlanning',
          'agentControl.stageRunLease.releasedAfterImplementation',
          'agentControl.stageRunLease.releasedAfterVerification'
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
  yield* sql`INSERT INTO main.agent_control_events_rebuild_062 SELECT * FROM main.agent_control_events`;
  yield* sql`DROP TABLE main.agent_control_events`;
  yield* sql`ALTER TABLE main.agent_control_events_rebuild_062 RENAME TO agent_control_events`;
  yield* restoreSchema(indexes);
  const sequence = sequenceRows[0]?.seq;
  if (sequence !== undefined) {
    yield* sql`
      DELETE FROM main.sqlite_sequence
      WHERE name IN ('agent_control_events', 'agent_control_events_rebuild_062')
    `;
    yield* sql`INSERT INTO main.sqlite_sequence(name, seq) VALUES ('agent_control_events', ${sequence})`;
  }
  yield* restoreSchema(triggers);
});

const rebuildTaskStates = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [triggers, indexes] = yield* Effect.all([
    sql<SchemaObject>`
      SELECT name, sql FROM main.sqlite_schema
      WHERE type = 'trigger' AND sql IS NOT NULL
        AND (tbl_name = 'agent_control_task_states' OR sql LIKE '%agent_control_task_states%')
      ORDER BY name
    `,
    sql<SchemaObject>`
      SELECT name, sql FROM main.sqlite_schema
      WHERE type = 'index' AND tbl_name = 'agent_control_task_states' AND sql IS NOT NULL
      ORDER BY name
    `,
  ]);
  for (const trigger of triggers) {
    yield* sql.unsafe(`DROP TRIGGER main.${quote(trigger.name)}`).unprepared;
  }
  yield* sql`
    CREATE TABLE main.agent_control_task_states_rebuild_062 (
      task_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      repository_node_id TEXT NOT NULL,
      issue_node_id TEXT NOT NULL,
      issue_number INTEGER NOT NULL CHECK (issue_number >= 1),
      issue_url TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN (
        'candidate', 'needs-attention', 'cancelled',
        'queued', 'running', 'waiting', 'succeeded', 'failed'
      )),
      source_gate TEXT NOT NULL CHECK (source_gate IN (
        'eligible', 'not-ready', 'paused', 'closed', 'timeline-invalid',
        'identity-invalid', 'source-missing'
      )),
      stage TEXT NOT NULL CHECK (stage IN ('intake', 'verification')),
      source_updated_at TEXT NOT NULL,
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      state_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK (revision >= 1),
      last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 1)
    )
  `;
  yield* sql`
    INSERT INTO main.agent_control_task_states_rebuild_062
    SELECT * FROM main.agent_control_task_states
  `;
  yield* sql`DROP TABLE main.agent_control_task_states`;
  yield* sql`
    ALTER TABLE main.agent_control_task_states_rebuild_062
    RENAME TO agent_control_task_states
  `;
  yield* restoreSchema(indexes);
  yield* restoreSchema(triggers);
});

const createCompanions = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE main.agent_control_task_verification_finalization_evidence (
      task_finalization_evidence_id TEXT PRIMARY KEY,
      receipt_id TEXT NOT NULL UNIQUE,
      marker_id TEXT NOT NULL UNIQUE,
      finalization_command_id TEXT NOT NULL UNIQUE,
      finalization_fingerprint TEXT NOT NULL UNIQUE,
      finalization_json TEXT NOT NULL UNIQUE CHECK (json_valid(finalization_json) = 1),
      verification_evidence_id TEXT NOT NULL UNIQUE,
      verification_receipt_id TEXT NOT NULL UNIQUE,
      verification_marker_id TEXT NOT NULL UNIQUE,
      verification_finalization_command_id TEXT NOT NULL UNIQUE,
      verification_finalization_fingerprint TEXT NOT NULL UNIQUE,
      verification_finalization_marker_fingerprint TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL UNIQUE,
      handoff_fingerprint TEXT NOT NULL UNIQUE,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL UNIQUE,
      verification_task_revision INTEGER NOT NULL CHECK (verification_task_revision >= 1),
      previous_task_revision INTEGER NOT NULL CHECK (
        previous_task_revision >= verification_task_revision
      ),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL,
      task_source_event_id TEXT NOT NULL,
      task_source_event_sequence INTEGER NOT NULL CHECK (task_source_event_sequence >= 1),
      task_source_event_stream_version INTEGER NOT NULL CHECK (
        task_source_event_stream_version = verification_task_revision
      ),
      delivery_terminal_state TEXT NOT NULL CHECK (
        delivery_terminal_state IN ('completed', 'failed', 'interrupted')
      ),
      verification_outcome TEXT NOT NULL CHECK (
        verification_outcome IN ('succeeded', 'failed', 'cancelled')
      ),
      terminal_cause TEXT NOT NULL CHECK (terminal_cause IN (
        'verification-passed', 'verification-failed', 'verification-invalid-output',
        'provider-delivery-failed', 'provider-delivery-interrupted'
      )),
      terminal_runtime_event_id TEXT NOT NULL UNIQUE,
      evaluation_authority TEXT NOT NULL CHECK (
        evaluation_authority IN ('accepted-evaluation', 'not-applicable')
      ),
      evaluation_id TEXT UNIQUE,
      evaluation_evidence_id TEXT UNIQUE,
      evaluation_receipt_id TEXT UNIQUE,
      evaluation_marker_id TEXT UNIQUE,
      evaluation_disposition TEXT CHECK (
        evaluation_disposition IS NULL OR evaluation_disposition IN ('evaluated', 'invalid-output')
      ),
      verification_verdict TEXT CHECK (
        verification_verdict IS NULL OR verification_verdict IN ('passed', 'failed')
      ),
      invalid_output_code TEXT CHECK (invalid_output_code IS NULL OR invalid_output_code IN (
        'missing-final-message', 'output-too-large', 'invalid-utf8',
        'malformed-json', 'unsupported-schema-version', 'schema-violation'
      )),
      terminal_stage_run_id TEXT NOT NULL,
      terminal_stage_event_id TEXT NOT NULL UNIQUE,
      terminal_stage_event_sequence INTEGER NOT NULL UNIQUE CHECK (
        terminal_stage_event_sequence >= 1
      ),
      terminal_stage_event_stream_version INTEGER NOT NULL CHECK (
        terminal_stage_event_stream_version = 3
      ),
      released_lease_id TEXT NOT NULL,
      released_lease_event_id TEXT NOT NULL UNIQUE,
      released_lease_event_sequence INTEGER NOT NULL UNIQUE CHECK (
        released_lease_event_sequence >= 1
      ),
      released_lease_event_stream_version INTEGER NOT NULL CHECK (
        released_lease_event_stream_version >= 2
      ),
      task_event_id TEXT NOT NULL UNIQUE,
      task_event_sequence INTEGER NOT NULL UNIQUE CHECK (task_event_sequence >= 1),
      task_event_stream_version INTEGER NOT NULL CHECK (
        task_event_stream_version = previous_task_revision + 1
      ),
      finalized_at TEXT NOT NULL,
      CHECK (
        (delivery_terminal_state = 'completed' AND verification_outcome = 'succeeded'
          AND terminal_cause = 'verification-passed'
          AND evaluation_authority = 'accepted-evaluation'
          AND evaluation_id IS NOT NULL AND evaluation_evidence_id IS NOT NULL
          AND evaluation_receipt_id IS NOT NULL AND evaluation_marker_id IS NOT NULL
          AND evaluation_disposition = 'evaluated' AND verification_verdict = 'passed'
          AND invalid_output_code IS NULL)
        OR (delivery_terminal_state = 'completed' AND verification_outcome = 'failed'
          AND terminal_cause = 'verification-failed'
          AND evaluation_authority = 'accepted-evaluation'
          AND evaluation_id IS NOT NULL AND evaluation_evidence_id IS NOT NULL
          AND evaluation_receipt_id IS NOT NULL AND evaluation_marker_id IS NOT NULL
          AND evaluation_disposition = 'evaluated' AND verification_verdict = 'failed'
          AND invalid_output_code IS NULL)
        OR (delivery_terminal_state = 'completed' AND verification_outcome = 'failed'
          AND terminal_cause = 'verification-invalid-output'
          AND evaluation_authority = 'accepted-evaluation'
          AND evaluation_id IS NOT NULL AND evaluation_evidence_id IS NOT NULL
          AND evaluation_receipt_id IS NOT NULL AND evaluation_marker_id IS NOT NULL
          AND evaluation_disposition = 'invalid-output' AND verification_verdict IS NULL
          AND invalid_output_code IS NOT NULL)
        OR (delivery_terminal_state = 'failed' AND verification_outcome = 'failed'
          AND terminal_cause = 'provider-delivery-failed'
          AND evaluation_authority = 'not-applicable'
          AND evaluation_id IS NULL AND evaluation_evidence_id IS NULL
          AND evaluation_receipt_id IS NULL AND evaluation_marker_id IS NULL
          AND evaluation_disposition IS NULL AND verification_verdict IS NULL
          AND invalid_output_code IS NULL)
        OR (delivery_terminal_state = 'interrupted' AND verification_outcome = 'cancelled'
          AND terminal_cause = 'provider-delivery-interrupted'
          AND evaluation_authority = 'not-applicable'
          AND evaluation_id IS NULL AND evaluation_evidence_id IS NULL
          AND evaluation_receipt_id IS NULL AND evaluation_marker_id IS NULL
          AND evaluation_disposition IS NULL AND verification_verdict IS NULL
          AND invalid_output_code IS NULL)
      ),
      FOREIGN KEY (verification_evidence_id)
        REFERENCES agent_control_verification_finalization_evidence(finalization_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (verification_receipt_id)
        REFERENCES agent_control_verification_finalization_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (verification_marker_id)
        REFERENCES agent_control_verification_finalization_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (task_source_event_id, task_id, task_source_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (terminal_stage_event_id, terminal_stage_run_id,
        terminal_stage_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (released_lease_event_id, released_lease_id,
        released_lease_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (task_event_id, task_id, task_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (receipt_id)
        REFERENCES agent_control_task_verification_finalization_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id)
        REFERENCES agent_control_task_verification_finalization_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_task_verification_finalization_receipts (
      receipt_id TEXT PRIMARY KEY,
      marker_id TEXT NOT NULL UNIQUE,
      task_finalization_evidence_id TEXT NOT NULL UNIQUE,
      finalization_command_id TEXT NOT NULL UNIQUE,
      finalization_fingerprint TEXT NOT NULL UNIQUE,
      verification_marker_id TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL UNIQUE,
      task_id TEXT NOT NULL UNIQUE,
      task_event_id TEXT NOT NULL UNIQUE,
      task_event_sequence INTEGER NOT NULL UNIQUE CHECK (task_event_sequence >= 1),
      task_event_stream_version INTEGER NOT NULL CHECK (task_event_stream_version >= 2),
      status TEXT NOT NULL CHECK (status = 'accepted'),
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (task_finalization_evidence_id)
        REFERENCES agent_control_task_verification_finalization_evidence(task_finalization_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id)
        REFERENCES agent_control_task_verification_finalization_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_task_verification_finalization_markers (
      marker_id TEXT PRIMARY KEY,
      marker_fingerprint TEXT NOT NULL UNIQUE,
      receipt_id TEXT NOT NULL UNIQUE,
      task_finalization_evidence_id TEXT NOT NULL UNIQUE,
      finalization_command_id TEXT NOT NULL UNIQUE,
      finalization_fingerprint TEXT NOT NULL UNIQUE,
      verification_marker_id TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL UNIQUE,
      task_id TEXT NOT NULL UNIQUE,
      task_event_id TEXT NOT NULL UNIQUE,
      task_event_sequence INTEGER NOT NULL UNIQUE CHECK (task_event_sequence >= 1),
      task_event_stream_version INTEGER NOT NULL CHECK (task_event_stream_version >= 2),
      committed_at TEXT NOT NULL,
      FOREIGN KEY (receipt_id)
        REFERENCES agent_control_task_verification_finalization_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (task_finalization_evidence_id)
        REFERENCES agent_control_task_verification_finalization_evidence(task_finalization_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_task_verification_finalization_publications (
      handoff_id TEXT PRIMARY KEY,
      marker_id TEXT NOT NULL UNIQUE,
      task_finalization_evidence_id TEXT NOT NULL UNIQUE,
      task_id TEXT NOT NULL UNIQUE,
      task_event_id TEXT NOT NULL UNIQUE,
      task_event_stream_version INTEGER NOT NULL CHECK (task_event_stream_version >= 2),
      publication_owner_id TEXT CHECK (
        publication_owner_id IS NULL OR (
          typeof(publication_owner_id) = 'text'
          AND length(publication_owner_id) = 36
          AND publication_owner_id GLOB '????????-????-????-????-????????????'
          AND publication_owner_id NOT GLOB '*[^0-9a-f-]*'
        )
      ),
      status TEXT NOT NULL CHECK (status IN ('pending', 'claimed', 'completed')),
      revision INTEGER NOT NULL CHECK (revision >= 1),
      claim_fence INTEGER NOT NULL CHECK (claim_fence >= 0),
      created_at TEXT NOT NULL,
      claimed_at TEXT,
      lease_expires_at TEXT,
      completed_at TEXT,
      CHECK (
        typeof(created_at) = 'text'
        AND COALESCE(created_at = strftime('%Y-%m-%dT%H:%M:%fZ', created_at), 0)
        AND (
          claimed_at IS NULL OR (
            typeof(claimed_at) = 'text'
            AND COALESCE(claimed_at = strftime('%Y-%m-%dT%H:%M:%fZ', claimed_at), 0)
          )
        )
        AND (
          lease_expires_at IS NULL OR (
            typeof(lease_expires_at) = 'text'
            AND COALESCE(
              lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', lease_expires_at), 0
            )
          )
        )
        AND (
          completed_at IS NULL OR (
            typeof(completed_at) = 'text'
            AND COALESCE(completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', completed_at), 0)
          )
        )
      ),
      CHECK (
        (status = 'pending' AND revision = 1 AND claim_fence = 0
          AND publication_owner_id IS NULL
          AND claimed_at IS NULL AND lease_expires_at IS NULL AND completed_at IS NULL)
        OR (status = 'claimed' AND revision >= 2 AND publication_owner_id IS NOT NULL
          AND typeof(publication_owner_id) = 'text'
          AND claim_fence >= 1 AND claimed_at IS NOT NULL AND lease_expires_at IS NOT NULL
          AND lease_expires_at > claimed_at AND completed_at IS NULL)
        OR (status = 'completed' AND revision >= 3 AND publication_owner_id IS NOT NULL
          AND typeof(publication_owner_id) = 'text'
          AND claim_fence >= 1 AND claimed_at IS NOT NULL AND lease_expires_at IS NOT NULL
          AND lease_expires_at > claimed_at AND completed_at IS NOT NULL
          AND completed_at >= claimed_at AND completed_at < lease_expires_at)
      ),
      FOREIGN KEY (marker_id)
        REFERENCES agent_control_task_verification_finalization_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (task_finalization_evidence_id)
        REFERENCES agent_control_task_verification_finalization_evidence(task_finalization_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (task_event_id, task_id, task_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
});

const createEventAndProjectionValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_finalization_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.aggregate_kind = 'task'
      AND NEW.event_type = 'agentControl.task.finalizedAfterVerification'
      AND NOT COALESCE((
        typeof(NEW.event_id) = 'text' AND length(NEW.event_id) > 0
        AND typeof(NEW.stream_id) = 'text' AND length(NEW.stream_id) > 0
        AND typeof(NEW.stream_version) = 'integer' AND NEW.stream_version >= 2
        AND typeof(NEW.occurred_at) = 'text'
        AND typeof(NEW.command_id) = 'text' AND length(NEW.command_id) > 0
        AND typeof(NEW.causation_event_id) = 'text' AND length(NEW.causation_event_id) > 0
        AND NEW.correlation_id = NEW.command_id
        AND NEW.actor_authority = 'system'
        AND typeof(NEW.payload_json) = 'text' AND json_valid(NEW.payload_json) = 1
        AND json(NEW.payload_json) = NEW.payload_json
        AND typeof(NEW.metadata_json) = 'text'
        AND NEW.metadata_json = '{"schemaVersion":1}'
        AND typeof(${TASK_FINALIZATION_PAYLOAD_STORAGE_FUNCTION}(
          NEW.event_type, CAST(NEW.payload_json AS BLOB), CAST(NEW.metadata_json AS BLOB),
          NEW.event_id, NEW.stream_version, NEW.command_id
        )) = 'blob'
        AND ${TASK_FINALIZATION_PAYLOAD_STORAGE_FUNCTION}(
          NEW.event_type, CAST(NEW.payload_json AS BLOB), CAST(NEW.metadata_json AS BLOB),
          NEW.event_id, NEW.stream_version, NEW.command_id
        ) = CAST(NEW.payload_json AS BLOB)
        AND (SELECT count(*) FROM json_each(NEW.payload_json)) = 35
        AND (SELECT count(DISTINCT key) FROM json_each(NEW.payload_json)) = 35
        AND (SELECT count(*) FROM json_each(NEW.payload_json) WHERE key IN (
          'projectId', 'taskId', 'verificationTaskRevision', 'previousTaskRevision',
          'githubIntakeSequence', 'sourceIdentityFingerprint', 'taskSourceEventId',
          'taskSourceEventSequence', 'taskSourceEventStreamVersion', 'handoffId',
          'handoffFingerprint', 'verificationFinalizationEvidenceId',
          'verificationFinalizationReceiptId', 'verificationFinalizationMarkerId',
          'verificationFinalizationCommandId', 'verificationFinalizationFingerprint',
          'verificationFinalizationMarkerFingerprint', 'terminalStageRunId',
          'terminalStageEventId', 'terminalStageEventSequence',
          'terminalStageEventStreamVersion', 'releasedLeaseId', 'releasedLeaseEventId',
          'releasedLeaseEventSequence', 'releasedLeaseEventStreamVersion',
          'terminalRuntimeEventId', 'taskFinalizationEvidenceId', 'deliveryTerminalState',
          'verificationOutcome', 'terminalCause', 'previousStatus', 'status', 'stage',
          'evaluation', 'finalizedAt'
        )) = 35
        AND json_type(NEW.payload_json, '$.evaluation') = 'object'
        AND (SELECT count(*) FROM json_each(NEW.payload_json, '$.evaluation')) = 8
        AND (SELECT count(DISTINCT key)
          FROM json_each(NEW.payload_json, '$.evaluation')) = 8
        AND (SELECT count(*) FROM json_each(NEW.payload_json, '$.evaluation') WHERE key IN (
          'evaluationAuthority', 'evaluationId', 'evaluationEvidenceId', 'evaluationReceiptId',
          'evaluationMarkerId', 'evaluationDisposition', 'verificationVerdict',
          'invalidOutputCode'
        )) = 8
        AND json_type(NEW.payload_json, '$.verificationTaskRevision') = 'integer'
        AND json_type(NEW.payload_json, '$.previousTaskRevision') = 'integer'
        AND json_type(NEW.payload_json, '$.githubIntakeSequence') = 'integer'
        AND json_type(NEW.payload_json, '$.taskSourceEventSequence') = 'integer'
        AND json_type(NEW.payload_json, '$.taskSourceEventStreamVersion') = 'integer'
        AND json_type(NEW.payload_json, '$.terminalStageEventSequence') = 'integer'
        AND json_type(NEW.payload_json, '$.terminalStageEventStreamVersion') = 'integer'
        AND json_type(NEW.payload_json, '$.releasedLeaseEventSequence') = 'integer'
        AND json_type(NEW.payload_json, '$.releasedLeaseEventStreamVersion') = 'integer'
        AND json_extract(NEW.payload_json, '$.projectId') IS NOT NULL
        AND json_extract(NEW.payload_json, '$.taskId') IS NEW.stream_id
        AND json_extract(NEW.payload_json, '$.previousTaskRevision') + 1 IS NEW.stream_version
        AND json_extract(NEW.payload_json, '$.taskSourceEventStreamVersion') IS
          json_extract(NEW.payload_json, '$.verificationTaskRevision')
        AND json_extract(NEW.payload_json, '$.terminalRuntimeEventId') IS NEW.causation_event_id
        AND json_extract(NEW.payload_json, '$.finalizedAt') IS NEW.occurred_at
        AND json_extract(NEW.payload_json, '$.stage') = 'verification'
        AND json_extract(NEW.payload_json, '$.status') IS
          json_extract(NEW.payload_json, '$.verificationOutcome')
        AND EXISTS (
          SELECT 1
          FROM main.agent_control_verification_finalization_evidence evidence
          JOIN main.agent_control_verification_finalization_receipts receipt
            ON receipt.receipt_id = evidence.receipt_id
           AND receipt.finalization_evidence_id = evidence.finalization_evidence_id
           AND receipt.status = 'accepted'
          JOIN main.agent_control_verification_finalization_markers marker
            ON marker.marker_id = evidence.marker_id
           AND marker.receipt_id = receipt.receipt_id
           AND marker.finalization_evidence_id = evidence.finalization_evidence_id
          JOIN main.agent_control_events source_event
            ON source_event.aggregate_kind = 'task'
           AND source_event.stream_id = evidence.task_id
           AND source_event.stream_version = evidence.task_revision
          JOIN main.agent_control_task_states state
            ON state.task_id = evidence.task_id
          WHERE evidence.handoff_id = json_extract(NEW.payload_json, '$.handoffId')
            AND evidence.handoff_fingerprint =
              json_extract(NEW.payload_json, '$.handoffFingerprint')
            AND evidence.finalization_evidence_id =
              json_extract(NEW.payload_json, '$.verificationFinalizationEvidenceId')
            AND receipt.receipt_id =
              json_extract(NEW.payload_json, '$.verificationFinalizationReceiptId')
            AND marker.marker_id =
              json_extract(NEW.payload_json, '$.verificationFinalizationMarkerId')
            AND evidence.finalization_command_id =
              json_extract(NEW.payload_json, '$.verificationFinalizationCommandId')
            AND evidence.finalization_fingerprint =
              json_extract(NEW.payload_json, '$.verificationFinalizationFingerprint')
            AND marker.marker_fingerprint =
              json_extract(NEW.payload_json, '$.verificationFinalizationMarkerFingerprint')
            AND evidence.project_id = json_extract(NEW.payload_json, '$.projectId')
            AND evidence.task_id = NEW.stream_id
            AND evidence.task_revision =
              json_extract(NEW.payload_json, '$.verificationTaskRevision')
            AND evidence.github_intake_sequence =
              json_extract(NEW.payload_json, '$.githubIntakeSequence')
            AND evidence.source_identity_fingerprint =
              json_extract(NEW.payload_json, '$.sourceIdentityFingerprint')
            AND source_event.event_id = json_extract(NEW.payload_json, '$.taskSourceEventId')
            AND source_event.sequence =
              json_extract(NEW.payload_json, '$.taskSourceEventSequence')
            AND source_event.stream_version =
              json_extract(NEW.payload_json, '$.taskSourceEventStreamVersion')
            AND evidence.delivery_terminal_state =
              json_extract(NEW.payload_json, '$.deliveryTerminalState')
            AND evidence.outcome = json_extract(NEW.payload_json, '$.verificationOutcome')
            AND evidence.terminal_cause = json_extract(NEW.payload_json, '$.terminalCause')
            AND evidence.evaluation_authority =
              json_extract(NEW.payload_json, '$.evaluation.evaluationAuthority')
            AND evidence.evaluation_id IS
              json_extract(NEW.payload_json, '$.evaluation.evaluationId')
            AND evidence.evaluation_evidence_id IS
              json_extract(NEW.payload_json, '$.evaluation.evaluationEvidenceId')
            AND evidence.evaluation_receipt_id IS
              json_extract(NEW.payload_json, '$.evaluation.evaluationReceiptId')
            AND evidence.evaluation_marker_id IS
              json_extract(NEW.payload_json, '$.evaluation.evaluationMarkerId')
            AND evidence.evaluation_disposition IS
              json_extract(NEW.payload_json, '$.evaluation.evaluationDisposition')
            AND evidence.verification_verdict IS
              json_extract(NEW.payload_json, '$.evaluation.verificationVerdict')
            AND evidence.invalid_output_code IS
              json_extract(NEW.payload_json, '$.evaluation.invalidOutputCode')
            AND evidence.terminal_runtime_event_id = NEW.causation_event_id
            AND evidence.stage_event_id =
              json_extract(NEW.payload_json, '$.terminalStageEventId')
            AND evidence.stage_run_id =
              json_extract(NEW.payload_json, '$.terminalStageRunId')
            AND evidence.stage_event_sequence =
              json_extract(NEW.payload_json, '$.terminalStageEventSequence')
            AND evidence.stage_event_stream_version =
              json_extract(NEW.payload_json, '$.terminalStageEventStreamVersion')
            AND evidence.lease_event_id =
              json_extract(NEW.payload_json, '$.releasedLeaseEventId')
            AND evidence.lease_id = json_extract(NEW.payload_json, '$.releasedLeaseId')
            AND evidence.lease_event_sequence =
              json_extract(NEW.payload_json, '$.releasedLeaseEventSequence')
            AND evidence.lease_event_stream_version =
              json_extract(NEW.payload_json, '$.releasedLeaseEventStreamVersion')
            AND evidence.finalized_at = NEW.occurred_at
            AND state.project_id = evidence.project_id
            AND state.revision = json_extract(NEW.payload_json, '$.previousTaskRevision')
            AND state.status = json_extract(NEW.payload_json, '$.previousStatus')
            AND state.stage = 'intake'
            AND state.status NOT IN ('succeeded', 'failed', 'cancelled')
        )
      ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid task Verification finalization event'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_finalization_event_no_update
    BEFORE UPDATE ON agent_control_events
    WHEN OLD.aggregate_kind = 'task'
      AND OLD.event_type = 'agentControl.task.finalizedAfterVerification'
    BEGIN SELECT RAISE(ABORT, 'task Verification finalization events are immutable'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_finalization_event_no_delete
    BEFORE DELETE ON agent_control_events
    WHEN OLD.aggregate_kind = 'task'
      AND OLD.event_type = 'agentControl.task.finalizedAfterVerification'
    BEGIN SELECT RAISE(ABORT, 'task Verification finalization events are immutable'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_projection_insert_reject
    BEFORE INSERT ON agent_control_task_states
    WHEN NEW.stage = 'verification'
    BEGIN SELECT RAISE(ABORT, 'terminal task projections require a prior intake projection'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_projection_no_regression
    BEFORE UPDATE ON agent_control_task_states
    WHEN OLD.stage = 'verification'
      AND (NEW.stage <> 'verification' OR NEW.status <> OLD.status)
    BEGIN SELECT RAISE(ABORT, 'terminal task projection stage is immutable'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_projection_validate
    BEFORE UPDATE ON agent_control_task_states
    WHEN OLD.stage = 'intake' AND NEW.stage = 'verification'
      AND NOT COALESCE((
        NEW.task_id IS OLD.task_id AND NEW.project_id IS OLD.project_id
        AND NEW.repository_node_id IS OLD.repository_node_id
        AND NEW.issue_node_id IS OLD.issue_node_id
        AND NEW.issue_number IS OLD.issue_number AND NEW.issue_url IS OLD.issue_url
        AND NEW.source_gate IS OLD.source_gate
        AND NEW.source_updated_at IS OLD.source_updated_at
        AND NEW.github_intake_sequence IS OLD.github_intake_sequence
        AND NEW.created_at IS OLD.created_at
        AND NEW.revision = OLD.revision + 1
        AND NEW.status IN ('succeeded', 'failed', 'cancelled')
        AND typeof(NEW.state_json) = 'text' AND json_valid(NEW.state_json) = 1
        AND json(NEW.state_json) = NEW.state_json
        AND EXISTS (
          SELECT 1 FROM main.agent_control_events event
          WHERE event.aggregate_kind = 'task' AND event.stream_id = NEW.task_id
            AND event.stream_version = NEW.revision
            AND event.sequence = NEW.last_event_sequence
            AND event.event_type = 'agentControl.task.finalizedAfterVerification'
            AND event.actor_authority = 'system'
            AND event.occurred_at = NEW.updated_at
            AND json_extract(event.payload_json, '$.previousTaskRevision') = OLD.revision
            AND json_extract(event.payload_json, '$.previousStatus') = OLD.status
            AND json_extract(event.payload_json, '$.status') = NEW.status
            AND json_extract(event.payload_json, '$.stage') = NEW.stage
            AND ${TASK_FINALIZATION_PROJECTION_MATCH_FUNCTION}(
              CAST(OLD.state_json AS BLOB), CAST(NEW.state_json AS BLOB),
              CAST(event.payload_json AS BLOB), event.sequence
            ) = 1
        )
      ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid task Verification terminal projection'); END
  `).unprepared;
});

const createCompanionValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_finalization_evidence_validate
    BEFORE INSERT ON agent_control_task_verification_finalization_evidence
    WHEN NOT COALESCE((
      typeof(NEW.task_finalization_evidence_id) = 'text'
      AND typeof(NEW.receipt_id) = 'text' AND typeof(NEW.marker_id) = 'text'
      AND typeof(NEW.finalization_command_id) = 'text'
      AND typeof(NEW.finalization_fingerprint) = 'text'
      AND length(NEW.finalization_fingerprint) = 64
      AND NEW.finalization_fingerprint NOT GLOB '*[^0-9a-f]*'
      AND typeof(NEW.verification_finalization_command_id) = 'text'
      AND length(NEW.verification_finalization_command_id) > 0
      AND typeof(NEW.verification_finalization_marker_fingerprint) = 'text'
      AND length(NEW.verification_finalization_marker_fingerprint) = 64
      AND NEW.verification_finalization_marker_fingerprint NOT GLOB '*[^0-9a-f]*'
      AND typeof(NEW.finalization_json) = 'text'
      AND json_valid(NEW.finalization_json) = 1 AND json(NEW.finalization_json) = NEW.finalization_json
      AND (
        SELECT count(*)
        FROM main.agent_control_events task_event
        WHERE task_event.event_id = NEW.task_event_id
          AND task_event.aggregate_kind = 'task'
          AND task_event.stream_id = NEW.task_id
          AND task_event.stream_version = NEW.task_event_stream_version
          AND task_event.sequence = NEW.task_event_sequence
          AND typeof(${TASK_FINALIZATION_DOCUMENT_STORAGE_FUNCTION}(
            CAST(NEW.finalization_json AS BLOB), CAST(task_event.payload_json AS BLOB),
            task_event.event_id, task_event.stream_version, NEW.finalization_command_id,
            NEW.task_finalization_evidence_id, NEW.receipt_id, NEW.marker_id,
            NEW.finalization_fingerprint
          )) = 'blob'
          AND ${TASK_FINALIZATION_DOCUMENT_STORAGE_FUNCTION}(
            CAST(NEW.finalization_json AS BLOB), CAST(task_event.payload_json AS BLOB),
            task_event.event_id, task_event.stream_version, NEW.finalization_command_id,
            NEW.task_finalization_evidence_id, NEW.receipt_id, NEW.marker_id,
            NEW.finalization_fingerprint
          ) = CAST(NEW.finalization_json AS BLOB)
      ) = 1
      AND json_extract(NEW.finalization_json, '$.commandId') = NEW.finalization_command_id
      AND json_extract(NEW.finalization_json, '$.taskFinalizationEvidenceId') =
        NEW.task_finalization_evidence_id
      AND json_extract(NEW.finalization_json, '$.taskFinalizationReceiptId') = NEW.receipt_id
      AND json_extract(NEW.finalization_json, '$.taskFinalizationMarkerId') = NEW.marker_id
      AND json_extract(NEW.finalization_json, '$.verificationFinalizationEvidenceId') =
        NEW.verification_evidence_id
      AND json_extract(NEW.finalization_json, '$.verificationFinalizationReceiptId') =
        NEW.verification_receipt_id
      AND json_extract(NEW.finalization_json, '$.verificationFinalizationMarkerId') =
        NEW.verification_marker_id
      AND json_extract(NEW.finalization_json, '$.verificationFinalizationCommandId') =
        NEW.verification_finalization_command_id
      AND json_extract(NEW.finalization_json, '$.verificationFinalizationFingerprint') =
        NEW.verification_finalization_fingerprint
      AND json_extract(NEW.finalization_json, '$.verificationFinalizationMarkerFingerprint') =
        NEW.verification_finalization_marker_fingerprint
      AND json_extract(NEW.finalization_json, '$.taskEventId') = NEW.task_event_id
      AND json_extract(NEW.finalization_json, '$.taskEventStreamVersion') =
        NEW.task_event_stream_version
      AND json_extract(NEW.finalization_json, '$.finalizedAt') = NEW.finalized_at
      AND EXISTS (
        SELECT 1
        FROM main.agent_control_verification_finalization_evidence verification
        JOIN main.agent_control_verification_finalization_receipts verification_receipt
          ON verification_receipt.receipt_id = verification.receipt_id
         AND verification_receipt.finalization_evidence_id = verification.finalization_evidence_id
         AND verification_receipt.status = 'accepted'
        JOIN main.agent_control_verification_finalization_markers verification_marker
          ON verification_marker.marker_id = verification.marker_id
         AND verification_marker.receipt_id = verification_receipt.receipt_id
         AND verification_marker.finalization_evidence_id = verification.finalization_evidence_id
        JOIN main.agent_control_events task_event
          ON task_event.event_id = NEW.task_event_id
         AND task_event.aggregate_kind = 'task' AND task_event.stream_id = NEW.task_id
         AND task_event.stream_version = NEW.task_event_stream_version
         AND task_event.sequence = NEW.task_event_sequence
        JOIN main.agent_control_task_states state ON state.task_id = NEW.task_id
        WHERE verification.finalization_evidence_id = NEW.verification_evidence_id
          AND verification_receipt.receipt_id = NEW.verification_receipt_id
          AND verification_marker.marker_id = NEW.verification_marker_id
          AND verification.finalization_command_id = NEW.verification_finalization_command_id
          AND verification.finalization_fingerprint = NEW.verification_finalization_fingerprint
          AND verification_marker.marker_fingerprint =
            NEW.verification_finalization_marker_fingerprint
          AND verification.handoff_id = NEW.handoff_id
          AND verification.handoff_fingerprint = NEW.handoff_fingerprint
          AND verification.project_id = NEW.project_id AND verification.task_id = NEW.task_id
          AND verification.task_revision = NEW.verification_task_revision
          AND verification.github_intake_sequence = NEW.github_intake_sequence
          AND verification.source_identity_fingerprint = NEW.source_identity_fingerprint
          AND verification.delivery_terminal_state = NEW.delivery_terminal_state
          AND verification.outcome = NEW.verification_outcome
          AND verification.terminal_cause = NEW.terminal_cause
          AND verification.evaluation_authority = NEW.evaluation_authority
          AND verification.evaluation_id IS NEW.evaluation_id
          AND verification.evaluation_evidence_id IS NEW.evaluation_evidence_id
          AND verification.evaluation_receipt_id IS NEW.evaluation_receipt_id
          AND verification.evaluation_marker_id IS NEW.evaluation_marker_id
          AND verification.evaluation_disposition IS NEW.evaluation_disposition
          AND verification.verification_verdict IS NEW.verification_verdict
          AND verification.invalid_output_code IS NEW.invalid_output_code
          AND verification.terminal_runtime_event_id = NEW.terminal_runtime_event_id
          AND verification.stage_event_id = NEW.terminal_stage_event_id
          AND verification.stage_run_id = NEW.terminal_stage_run_id
          AND verification.stage_event_sequence = NEW.terminal_stage_event_sequence
          AND verification.stage_event_stream_version = NEW.terminal_stage_event_stream_version
          AND verification.lease_event_id = NEW.released_lease_event_id
          AND verification.lease_id = NEW.released_lease_id
          AND verification.lease_event_sequence = NEW.released_lease_event_sequence
          AND verification.lease_event_stream_version = NEW.released_lease_event_stream_version
          AND verification.finalized_at = NEW.finalized_at
          AND task_event.event_type = 'agentControl.task.finalizedAfterVerification'
          AND task_event.command_id = NEW.finalization_command_id
          AND task_event.occurred_at = NEW.finalized_at
          AND json_extract(task_event.payload_json, '$.taskFinalizationEvidenceId') =
            NEW.task_finalization_evidence_id
          AND state.revision = NEW.task_event_stream_version
          AND state.last_event_sequence = NEW.task_event_sequence
          AND state.stage = 'verification' AND state.status = NEW.verification_outcome
      )
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'task Verification finalization evidence is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_finalization_receipt_validate
    BEFORE INSERT ON agent_control_task_verification_finalization_receipts
    WHEN NOT COALESCE((EXISTS (
      SELECT 1 FROM main.agent_control_task_verification_finalization_evidence evidence
      WHERE evidence.task_finalization_evidence_id = NEW.task_finalization_evidence_id
        AND evidence.receipt_id = NEW.receipt_id AND evidence.marker_id = NEW.marker_id
        AND evidence.finalization_command_id = NEW.finalization_command_id
        AND evidence.finalization_fingerprint = NEW.finalization_fingerprint
        AND evidence.verification_marker_id = NEW.verification_marker_id
        AND evidence.handoff_id = NEW.handoff_id AND evidence.task_id = NEW.task_id
        AND evidence.task_event_id = NEW.task_event_id
        AND evidence.task_event_sequence = NEW.task_event_sequence
        AND evidence.task_event_stream_version = NEW.task_event_stream_version
        AND evidence.finalized_at = NEW.accepted_at AND NEW.status = 'accepted'
    )), 0)
    BEGIN SELECT RAISE(ABORT, 'task Verification finalization receipt is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_finalization_marker_validate
    BEFORE INSERT ON agent_control_task_verification_finalization_markers
    WHEN NOT COALESCE((EXISTS (
      SELECT 1
      FROM main.agent_control_task_verification_finalization_evidence evidence
      JOIN main.agent_control_task_verification_finalization_receipts receipt
        ON receipt.receipt_id = evidence.receipt_id
       AND receipt.task_finalization_evidence_id = evidence.task_finalization_evidence_id
      JOIN main.agent_control_task_verification_finalization_publications publication
        ON publication.marker_id = evidence.marker_id
       AND publication.task_finalization_evidence_id = evidence.task_finalization_evidence_id
      WHERE evidence.marker_id = NEW.marker_id AND receipt.marker_id = NEW.marker_id
        AND evidence.task_finalization_evidence_id = NEW.task_finalization_evidence_id
        AND receipt.task_finalization_evidence_id = NEW.task_finalization_evidence_id
        AND evidence.finalization_command_id = NEW.finalization_command_id
        AND receipt.finalization_command_id = NEW.finalization_command_id
        AND evidence.finalization_fingerprint = NEW.finalization_fingerprint
        AND receipt.finalization_fingerprint = NEW.finalization_fingerprint
        AND evidence.verification_marker_id = NEW.verification_marker_id
        AND receipt.verification_marker_id = NEW.verification_marker_id
        AND evidence.handoff_id = NEW.handoff_id AND receipt.handoff_id = NEW.handoff_id
        AND evidence.task_id = NEW.task_id AND receipt.task_id = NEW.task_id
        AND evidence.task_event_id = NEW.task_event_id
        AND receipt.task_event_id = NEW.task_event_id
        AND publication.task_event_id = NEW.task_event_id
        AND publication.task_event_stream_version = NEW.task_event_stream_version
        AND publication.handoff_id = NEW.handoff_id
        AND publication.task_id = NEW.task_id
        AND publication.status = 'pending' AND publication.revision = 1
        AND publication.claim_fence = 0
        AND publication.publication_owner_id IS NULL
        AND publication.created_at = NEW.committed_at
        AND publication.claimed_at IS NULL AND publication.lease_expires_at IS NULL
        AND publication.completed_at IS NULL
        AND evidence.task_event_sequence = NEW.task_event_sequence
        AND receipt.task_event_sequence = NEW.task_event_sequence
        AND evidence.task_event_stream_version = NEW.task_event_stream_version
        AND receipt.task_event_stream_version = NEW.task_event_stream_version
        AND evidence.finalized_at = NEW.committed_at
        AND receipt.accepted_at = NEW.committed_at AND receipt.status = 'accepted'
        AND typeof(NEW.marker_fingerprint) = 'text'
        AND length(NEW.marker_fingerprint) = 64
        AND NEW.marker_fingerprint NOT GLOB '*[^0-9a-f]*'
        AND ${TASK_FINALIZATION_MARKER_MATCH_FUNCTION}(
          CAST(evidence.finalization_json AS BLOB), NEW.marker_fingerprint
        ) = 1
    )), 0)
    BEGIN SELECT RAISE(ABORT, 'task Verification finalization marker is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_finalization_publication_insert_validate
    BEFORE INSERT ON agent_control_task_verification_finalization_publications
    WHEN NOT COALESCE((
      typeof(NEW.handoff_id) = 'text' AND typeof(NEW.marker_id) = 'text'
      AND typeof(NEW.task_finalization_evidence_id) = 'text'
      AND typeof(NEW.task_id) = 'text' AND typeof(NEW.task_event_id) = 'text'
      AND typeof(NEW.task_event_stream_version) = 'integer'
      AND NEW.status = 'pending' AND NEW.revision = 1
      AND typeof(NEW.claim_fence) = 'integer' AND NEW.claim_fence = 0
      AND NEW.publication_owner_id IS NULL
      AND typeof(NEW.created_at) = 'text'
      AND NEW.claimed_at IS NULL AND NEW.lease_expires_at IS NULL
      AND NEW.completed_at IS NULL
      AND EXISTS (
        SELECT 1
        FROM main.agent_control_task_verification_finalization_evidence evidence
        JOIN main.agent_control_task_verification_finalization_receipts receipt
          ON receipt.receipt_id = evidence.receipt_id
         AND receipt.task_finalization_evidence_id = evidence.task_finalization_evidence_id
         AND receipt.status = 'accepted'
        JOIN main.agent_control_events event
          ON event.event_id = evidence.task_event_id
         AND event.aggregate_kind = 'task'
         AND event.stream_id = evidence.task_id
         AND event.stream_version = evidence.task_event_stream_version
        WHERE evidence.handoff_id = NEW.handoff_id
          AND evidence.marker_id = NEW.marker_id
          AND evidence.task_finalization_evidence_id = NEW.task_finalization_evidence_id
          AND evidence.task_id = NEW.task_id
          AND evidence.task_event_id = NEW.task_event_id
          AND evidence.task_event_stream_version = NEW.task_event_stream_version
          AND evidence.finalized_at = NEW.created_at
          AND receipt.marker_id = NEW.marker_id
      )
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'task Verification finalization publication is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_finalization_publication_update_validate
    BEFORE UPDATE ON agent_control_task_verification_finalization_publications
    WHEN NOT COALESCE((
      NEW.handoff_id IS OLD.handoff_id AND NEW.marker_id IS OLD.marker_id
      AND NEW.task_finalization_evidence_id IS OLD.task_finalization_evidence_id
      AND NEW.task_id IS OLD.task_id AND NEW.task_event_id IS OLD.task_event_id
      AND NEW.task_event_stream_version IS OLD.task_event_stream_version
      AND NEW.created_at IS OLD.created_at AND NEW.revision = OLD.revision + 1
      AND typeof(NEW.claim_fence) = 'integer'
      AND typeof(NEW.publication_owner_id) = 'text'
      AND (
        (OLD.status = 'pending' AND NEW.status = 'claimed'
          AND OLD.publication_owner_id IS NULL AND NEW.publication_owner_id IS NOT NULL
          AND OLD.claim_fence = 0 AND NEW.claim_fence = 1
          AND OLD.claimed_at IS NULL AND NEW.claimed_at IS NOT NULL
          AND OLD.lease_expires_at IS NULL AND NEW.lease_expires_at IS NOT NULL
          AND NEW.lease_expires_at > NEW.claimed_at
          AND OLD.completed_at IS NULL AND NEW.completed_at IS NULL)
        OR (OLD.status = 'claimed' AND NEW.status = 'claimed'
          AND OLD.publication_owner_id IS NEW.publication_owner_id
          AND OLD.claim_fence = NEW.claim_fence
          AND OLD.claimed_at IS NOT NULL AND NEW.claimed_at >= OLD.claimed_at
          AND OLD.lease_expires_at IS NOT NULL AND OLD.lease_expires_at > NEW.claimed_at
          AND NEW.lease_expires_at IS NOT NULL
          AND NEW.lease_expires_at >= OLD.lease_expires_at
          AND NEW.lease_expires_at > NEW.claimed_at
          AND OLD.completed_at IS NULL AND NEW.completed_at IS NULL)
        OR (OLD.status = 'claimed' AND NEW.status = 'claimed'
          AND NEW.publication_owner_id IS NOT NULL
          AND NEW.claim_fence = OLD.claim_fence + 1
          AND OLD.claimed_at IS NOT NULL AND NEW.claimed_at >= OLD.lease_expires_at
          AND OLD.lease_expires_at IS NOT NULL
          AND NEW.lease_expires_at IS NOT NULL
          AND NEW.lease_expires_at > NEW.claimed_at
          AND OLD.completed_at IS NULL AND NEW.completed_at IS NULL)
        OR (OLD.status = 'claimed' AND NEW.status = 'completed'
          AND OLD.publication_owner_id IS NEW.publication_owner_id
          AND OLD.claim_fence = NEW.claim_fence
          AND OLD.claimed_at IS NEW.claimed_at
          AND OLD.lease_expires_at IS NEW.lease_expires_at
          AND OLD.completed_at IS NULL AND NEW.completed_at IS NOT NULL)
      )
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid task Verification publication transition'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_task_verification_finalization_publication_no_delete
    BEFORE DELETE ON agent_control_task_verification_finalization_publications
    BEGIN SELECT RAISE(ABORT, 'task Verification finalization publication is durable'); END
  `).unprepared;
  for (const table of [
    "agent_control_task_verification_finalization_evidence",
    "agent_control_task_verification_finalization_receipts",
    "agent_control_task_verification_finalization_markers",
  ]) {
    yield* sql.unsafe(`
      CREATE TRIGGER main.${quote(`${table}_no_update`)}
      BEFORE UPDATE ON ${quote(table)}
      BEGIN SELECT RAISE(ABORT, 'task Verification finalization authority is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.${quote(`${table}_no_delete`)}
      BEFORE DELETE ON ${quote(table)}
      BEGIN SELECT RAISE(ABORT, 'task Verification finalization authority is immutable'); END
    `).unprepared;
  }
});

export const makeMigration062 = (faultPoint?: Migration062FaultPoint) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const injectFault = (point: Migration062FaultPoint) =>
      faultPoint === point
        ? Effect.die(new Error(`migration 062 injected ${point} failure`))
        : Effect.void;
    const numericPayload = udfPayloadJson.replace(
      `"taskFinalizationEvidenceId":"${udfIds.evidenceId}"`,
      '"taskFinalizationEvidenceId":7',
    );
    const duplicatePayload = udfPayloadJson.replace(
      `"taskFinalizationEvidenceId":"${udfIds.evidenceId}"`,
      `"taskFinalizationEvidenceId":"${udfIds.evidenceId}","taskFinalizationEvidenceId":"attacker"`,
    );
    const duplicateDocument = udfDocumentJson.replace(
      '"schemaVersion":1',
      '"schemaVersion":1,"schemaVersion":1',
    );
    const udfPreflight = yield* sql<{
      readonly payloadType: string;
      readonly payloadBytes: number;
      readonly documentType: string;
      readonly documentBytes: number;
      readonly marker: number;
      readonly payloadText: string;
      readonly documentText: string;
      readonly numericPayload: string;
      readonly duplicatePayload: string;
      readonly duplicateDocument: string;
      readonly fingerprint: string;
      readonly projection: number;
      readonly projectionText: number;
      readonly projectionDivergent: number;
    }>`
      SELECT
        typeof(${sql.literal(TASK_FINALIZATION_PAYLOAD_STORAGE_FUNCTION)}(
          'agentControl.task.finalizedAfterVerification', CAST(${udfPayloadJson} AS BLOB),
          CAST('{"schemaVersion":1}' AS BLOB), ${udfIds.eventId}, 2, ${udfIds.commandId}
        )) AS "payloadType",
        ${sql.literal(TASK_FINALIZATION_PAYLOAD_STORAGE_FUNCTION)}(
          'agentControl.task.finalizedAfterVerification', CAST(${udfPayloadJson} AS BLOB),
          CAST('{"schemaVersion":1}' AS BLOB), ${udfIds.eventId}, 2, ${udfIds.commandId}
        ) = CAST(${udfPayloadJson} AS BLOB) AS "payloadBytes",
        typeof(${sql.literal(TASK_FINALIZATION_DOCUMENT_STORAGE_FUNCTION)}(
          CAST(${udfDocumentJson} AS BLOB), CAST(${udfPayloadJson} AS BLOB),
          ${udfIds.eventId}, 2, ${udfIds.commandId}, ${udfIds.evidenceId},
          ${udfIds.receiptId}, ${udfIds.markerId}, ${udfFinalizationFingerprint}
        )) AS "documentType",
        ${sql.literal(TASK_FINALIZATION_DOCUMENT_STORAGE_FUNCTION)}(
          CAST(${udfDocumentJson} AS BLOB), CAST(${udfPayloadJson} AS BLOB),
          ${udfIds.eventId}, 2, ${udfIds.commandId}, ${udfIds.evidenceId},
          ${udfIds.receiptId}, ${udfIds.markerId}, ${udfFinalizationFingerprint}
        ) = CAST(${udfDocumentJson} AS BLOB) AS "documentBytes",
        ${sql.literal(TASK_FINALIZATION_MARKER_MATCH_FUNCTION)}(
          CAST(${udfDocumentJson} AS BLOB), ${udfMarkerFingerprint}
        ) AS marker,
        typeof(${sql.literal(TASK_FINALIZATION_PAYLOAD_STORAGE_FUNCTION)}(
          'agentControl.task.finalizedAfterVerification', ${udfPayloadJson},
          CAST('{"schemaVersion":1}' AS BLOB), ${udfIds.eventId}, 2, ${udfIds.commandId}
        )) AS "payloadText",
        typeof(${sql.literal(TASK_FINALIZATION_DOCUMENT_STORAGE_FUNCTION)}(
          ${udfDocumentJson}, CAST(${udfPayloadJson} AS BLOB), ${udfIds.eventId}, 2,
          ${udfIds.commandId}, ${udfIds.evidenceId}, ${udfIds.receiptId}, ${udfIds.markerId},
          ${udfFinalizationFingerprint}
        )) AS "documentText",
        typeof(${sql.literal(TASK_FINALIZATION_PAYLOAD_STORAGE_FUNCTION)}(
          'agentControl.task.finalizedAfterVerification', CAST(${numericPayload} AS BLOB),
          CAST('{"schemaVersion":1}' AS BLOB), ${udfIds.eventId}, 2, ${udfIds.commandId}
        )) AS "numericPayload",
        typeof(${sql.literal(TASK_FINALIZATION_PAYLOAD_STORAGE_FUNCTION)}(
          'agentControl.task.finalizedAfterVerification', CAST(${duplicatePayload} AS BLOB),
          CAST('{"schemaVersion":1}' AS BLOB), ${udfIds.eventId}, 2, ${udfIds.commandId}
        )) AS "duplicatePayload",
        typeof(${sql.literal(TASK_FINALIZATION_DOCUMENT_STORAGE_FUNCTION)}(
          CAST(${duplicateDocument} AS BLOB), CAST(${udfPayloadJson} AS BLOB),
          ${udfIds.eventId}, 2, ${udfIds.commandId}, ${udfIds.evidenceId},
          ${udfIds.receiptId}, ${udfIds.markerId}, ${udfFinalizationFingerprint}
        )) AS "duplicateDocument",
        typeof(${sql.literal(TASK_FINALIZATION_DOCUMENT_STORAGE_FUNCTION)}(
          CAST(${udfDocumentJson} AS BLOB), CAST(${udfPayloadJson} AS BLOB),
          ${udfIds.eventId}, 2, ${udfIds.commandId}, ${udfIds.evidenceId},
          ${udfIds.receiptId}, ${udfIds.markerId}, ${"f".repeat(64)}
        )) AS fingerprint,
        ${sql.literal(TASK_FINALIZATION_PROJECTION_MATCH_FUNCTION)}(
          CAST(${udfOldTaskStateJson} AS BLOB), CAST(${udfNewTaskStateJson} AS BLOB),
          CAST(${udfPayloadJson} AS BLOB), 2
        ) AS projection,
        ${sql.literal(TASK_FINALIZATION_PROJECTION_MATCH_FUNCTION)}(
          ${udfOldTaskStateJson}, CAST(${udfNewTaskStateJson} AS BLOB),
          CAST(${udfPayloadJson} AS BLOB), 2
        ) AS "projectionText",
        ${sql.literal(TASK_FINALIZATION_PROJECTION_MATCH_FUNCTION)}(
          CAST(${udfOldTaskStateJson} AS BLOB), CAST(${udfDivergentTaskStateJson} AS BLOB),
          CAST(${udfPayloadJson} AS BLOB), 2
        ) AS "projectionDivergent"
    `;
    if (
      udfPreflight.length !== 1 ||
      udfPreflight[0]?.payloadType !== "blob" ||
      udfPreflight[0]?.payloadBytes !== 1 ||
      udfPreflight[0]?.documentType !== "blob" ||
      udfPreflight[0]?.documentBytes !== 1 ||
      udfPreflight[0]?.marker !== 1 ||
      udfPreflight[0]?.payloadText !== "null" ||
      udfPreflight[0]?.documentText !== "null" ||
      udfPreflight[0]?.numericPayload !== "null" ||
      udfPreflight[0]?.duplicatePayload !== "null" ||
      udfPreflight[0]?.duplicateDocument !== "null" ||
      udfPreflight[0]?.fingerprint !== "null" ||
      udfPreflight[0]?.projection !== 1 ||
      udfPreflight[0]?.projectionText !== 0 ||
      udfPreflight[0]?.projectionDivergent !== 0
    ) {
      return yield* Effect.die(
        new Error("migration 062 requires BLOB-preserving duplicate-safe Task finalization UDFs"),
      );
    }
    yield* sql`PRAGMA defer_foreign_keys = ON`;
    yield* injectFault("before-events-rebuild");
    yield* rebuildAgentControlEvents;
    yield* injectFault("after-events-rebuild");
    yield* rebuildTaskStates;
    yield* injectFault("after-task-states-rebuild");
    yield* createCompanions;
    yield* injectFault("after-companions");
    yield* createEventAndProjectionValidation;
    yield* createCompanionValidation;
    yield* injectFault("after-install");
    const foreignKeyViolations = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
    if (foreignKeyViolations.length !== 0) {
      return yield* Effect.die(new Error("migration 062 introduced foreign-key violations"));
    }
    const integrity = yield* sql<{ readonly integrity_check: string }>`PRAGMA integrity_check`;
    if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") {
      return yield* Effect.die(new Error("migration 062 failed integrity_check"));
    }
    yield* sql`PRAGMA defer_foreign_keys = OFF`;
  });

export default makeMigration062();
