import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const quote = (identifier: string) => `"${identifier.replaceAll('"', '""')}"`;
const text = (column: string) =>
  `typeof(${column}) = 'text' AND length(${column}) > 0 AND instr(${column}, char(0)) = 0`;
const integer = (column: string) => `typeof(${column}) = 'integer' AND ${column} >= 1`;
const sha256 = (column: string) =>
  `${text(column)} AND length(${column}) = 64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;
const timestamp = (column: string) => `
  ${text(column)}
  AND length(${column}) = 24
  AND ${column} GLOB
    '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9]Z'
  AND strftime('%Y-%m-%dT%H:%M:%fZ', ${column}) = ${column}
`;

export type Migration061FaultPoint =
  | "before-events-rebuild"
  | "after-events-rebuild"
  | "after-companions"
  | "after-install";

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
    CREATE TABLE main.agent_control_events_rebuild_061 (
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
  yield* sql`INSERT INTO main.agent_control_events_rebuild_061 SELECT * FROM main.agent_control_events`;
  yield* sql`DROP TABLE main.agent_control_events`;
  yield* sql`ALTER TABLE main.agent_control_events_rebuild_061 RENAME TO agent_control_events`;
  yield* restoreSchema(indexes);
  const sequence = sequenceRows[0]?.seq;
  if (sequence !== undefined) {
    yield* sql`
      DELETE FROM main.sqlite_sequence
      WHERE name IN ('agent_control_events', 'agent_control_events_rebuild_061')
    `;
    yield* sql`INSERT INTO main.sqlite_sequence(name, seq) VALUES ('agent_control_events', ${sequence})`;
  }
  yield* restoreSchema(triggers);
});

const createCompanions = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE main.agent_control_verification_finalization_evidence (
      finalization_evidence_id TEXT PRIMARY KEY,
      receipt_id TEXT NOT NULL UNIQUE,
      marker_id TEXT NOT NULL UNIQUE,
      finalization_command_id TEXT NOT NULL UNIQUE,
      finalization_fingerprint TEXT NOT NULL UNIQUE,
      finalization_json TEXT NOT NULL UNIQUE CHECK (json_valid(finalization_json) = 1),
      handoff_id TEXT NOT NULL UNIQUE,
      handoff_fingerprint TEXT NOT NULL UNIQUE,
      project_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
      github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
      source_identity_fingerprint TEXT NOT NULL,
      stage_run_id TEXT NOT NULL UNIQUE,
      attempt_id TEXT NOT NULL UNIQUE,
      lease_id TEXT NOT NULL UNIQUE,
      lease_holder_id TEXT NOT NULL,
      fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
      provider_delivery_id TEXT NOT NULL UNIQUE,
      provider_instance_id TEXT NOT NULL,
      provider_turn_id TEXT NOT NULL,
      delivery_revision INTEGER NOT NULL CHECK (delivery_revision >= 1),
      delivery_terminal_state TEXT NOT NULL CHECK (
        delivery_terminal_state IN ('completed', 'failed', 'interrupted')
      ),
      terminal_runtime_event_id TEXT NOT NULL UNIQUE,
      terminal_at TEXT NOT NULL,
      start_evidence_id TEXT NOT NULL UNIQUE,
      start_receipt_id TEXT NOT NULL UNIQUE,
      start_marker_id TEXT NOT NULL UNIQUE,
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
      outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'cancelled')),
      terminal_cause TEXT NOT NULL CHECK (terminal_cause IN (
        'verification-passed', 'verification-failed', 'verification-invalid-output',
        'provider-delivery-failed', 'provider-delivery-interrupted'
      )),
      stage_event_id TEXT NOT NULL UNIQUE,
      stage_event_sequence INTEGER NOT NULL UNIQUE CHECK (stage_event_sequence >= 1),
      stage_event_stream_version INTEGER NOT NULL CHECK (stage_event_stream_version = 3),
      lease_event_id TEXT NOT NULL UNIQUE,
      lease_event_sequence INTEGER NOT NULL UNIQUE CHECK (lease_event_sequence >= 1),
      lease_event_stream_version INTEGER NOT NULL CHECK (lease_event_stream_version >= 2),
      finalized_at TEXT NOT NULL,
      CHECK (
        (delivery_terminal_state = 'completed'
          AND evaluation_authority = 'accepted-evaluation'
          AND evaluation_id IS NOT NULL AND evaluation_evidence_id IS NOT NULL
          AND evaluation_receipt_id IS NOT NULL AND evaluation_marker_id IS NOT NULL
          AND (
            (evaluation_disposition = 'evaluated' AND verification_verdict = 'passed'
              AND invalid_output_code IS NULL AND outcome = 'succeeded'
              AND terminal_cause = 'verification-passed')
            OR (evaluation_disposition = 'evaluated' AND verification_verdict = 'failed'
              AND invalid_output_code IS NULL AND outcome = 'failed'
              AND terminal_cause = 'verification-failed')
            OR (evaluation_disposition = 'invalid-output' AND verification_verdict IS NULL
              AND invalid_output_code IS NOT NULL AND outcome = 'failed'
              AND terminal_cause = 'verification-invalid-output')
          ))
        OR (delivery_terminal_state = 'failed' AND outcome = 'failed'
          AND terminal_cause = 'provider-delivery-failed'
          AND evaluation_authority = 'not-applicable' AND evaluation_id IS NULL
          AND evaluation_evidence_id IS NULL AND evaluation_receipt_id IS NULL
          AND evaluation_marker_id IS NULL AND evaluation_disposition IS NULL
          AND verification_verdict IS NULL AND invalid_output_code IS NULL)
        OR (delivery_terminal_state = 'interrupted' AND outcome = 'cancelled'
          AND terminal_cause = 'provider-delivery-interrupted'
          AND evaluation_authority = 'not-applicable' AND evaluation_id IS NULL
          AND evaluation_evidence_id IS NULL AND evaluation_receipt_id IS NULL
          AND evaluation_marker_id IS NULL AND evaluation_disposition IS NULL
          AND verification_verdict IS NULL AND invalid_output_code IS NULL)
      ),
      FOREIGN KEY (handoff_id)
        REFERENCES agent_control_verification_handoff_accepted(handoff_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (provider_delivery_id)
        REFERENCES agent_control_verification_deliveries(provider_delivery_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (start_evidence_id)
        REFERENCES agent_control_verification_stage_started_evidence(start_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (evaluation_id)
        REFERENCES agent_control_verification_evaluation_evidence(evaluation_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (evaluation_evidence_id)
        REFERENCES agent_control_verification_evaluation_evidence(evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (evaluation_receipt_id)
        REFERENCES agent_control_verification_evaluation_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (evaluation_marker_id)
        REFERENCES agent_control_verification_evaluation_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (stage_event_id, stage_run_id, stage_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (lease_event_id, lease_id, lease_event_stream_version)
        REFERENCES agent_control_events(event_id, stream_id, stream_version)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (receipt_id)
        REFERENCES agent_control_verification_finalization_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id)
        REFERENCES agent_control_verification_finalization_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_verification_finalization_receipts (
      receipt_id TEXT PRIMARY KEY,
      marker_id TEXT NOT NULL UNIQUE,
      finalization_evidence_id TEXT NOT NULL UNIQUE,
      finalization_command_id TEXT NOT NULL UNIQUE,
      finalization_fingerprint TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL UNIQUE,
      outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'cancelled')),
      terminal_cause TEXT NOT NULL CHECK (terminal_cause IN (
        'verification-passed', 'verification-failed', 'verification-invalid-output',
        'provider-delivery-failed', 'provider-delivery-interrupted'
      )),
      stage_event_id TEXT NOT NULL UNIQUE,
      stage_event_sequence INTEGER NOT NULL UNIQUE CHECK (stage_event_sequence >= 1),
      lease_event_id TEXT NOT NULL UNIQUE,
      lease_event_sequence INTEGER NOT NULL UNIQUE CHECK (lease_event_sequence >= 1),
      status TEXT NOT NULL CHECK (status = 'accepted'),
      accepted_at TEXT NOT NULL,
      FOREIGN KEY (finalization_evidence_id)
        REFERENCES agent_control_verification_finalization_evidence(finalization_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY (marker_id)
        REFERENCES agent_control_verification_finalization_markers(marker_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
    )
  `;
  yield* sql`
    CREATE TABLE main.agent_control_verification_finalization_markers (
      marker_id TEXT PRIMARY KEY,
      marker_fingerprint TEXT NOT NULL UNIQUE,
      receipt_id TEXT NOT NULL UNIQUE,
      finalization_evidence_id TEXT NOT NULL UNIQUE,
      finalization_command_id TEXT NOT NULL UNIQUE,
      finalization_fingerprint TEXT NOT NULL UNIQUE,
      handoff_id TEXT NOT NULL UNIQUE,
      committed_at TEXT NOT NULL,
      FOREIGN KEY (receipt_id)
        REFERENCES agent_control_verification_finalization_receipts(receipt_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
      FOREIGN KEY (finalization_evidence_id)
        REFERENCES agent_control_verification_finalization_evidence(finalization_evidence_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    )
  `;
  yield* sql`
    CREATE INDEX main.idx_agent_control_verification_finalization_candidates
    ON agent_control_verification_deliveries(handoff_id, state)
    WHERE state IN ('completed', 'failed', 'interrupted')
  `;
});

const createStorageAndImmutability = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_verification_finalization_evidence_storage_validate
    BEFORE INSERT ON agent_control_verification_finalization_evidence
    WHEN NOT COALESCE((
      ${[
        "finalization_evidence_id",
        "receipt_id",
        "marker_id",
        "finalization_command_id",
        "handoff_id",
        "project_id",
        "task_id",
        "stage_run_id",
        "attempt_id",
        "lease_id",
        "lease_holder_id",
        "provider_delivery_id",
        "provider_instance_id",
        "provider_turn_id",
        "delivery_terminal_state",
        "terminal_runtime_event_id",
        "start_evidence_id",
        "start_receipt_id",
        "start_marker_id",
        "evaluation_authority",
        "outcome",
        "terminal_cause",
        "stage_event_id",
        "lease_event_id",
      ]
        .map((column) => text(`NEW.${column}`))
        .join(" AND ")}
      AND ${["finalization_fingerprint", "handoff_fingerprint", "source_identity_fingerprint"]
        .map((column) => sha256(`NEW.${column}`))
        .join(" AND ")}
      AND typeof(NEW.finalization_json) = 'text'
      AND json_valid(NEW.finalization_json) = 1
      AND json(NEW.finalization_json) = NEW.finalization_json
      AND json_type(NEW.finalization_json, '$') = 'object'
      AND json_type(NEW.finalization_json, '$.report') IS NULL
      AND NOT EXISTS (
        SELECT 1 FROM json_each(NEW.finalization_json)
        WHERE key NOT IN (
          'schemaVersion', 'handoffId', 'handoffFingerprint', 'finalizationCommandId',
          'finalizationEvidenceId', 'outcome', 'terminalCause', 'deliveryTerminalState',
          'terminalRuntimeEventId', 'evaluation', 'stageEventId', 'stageEventSequence',
          'leaseEventId', 'leaseEventSequence', 'finalizedAt'
        )
      )
      AND ${[
        "task_revision",
        "github_intake_sequence",
        "fence_token",
        "delivery_revision",
        "stage_event_sequence",
        "stage_event_stream_version",
        "lease_event_sequence",
        "lease_event_stream_version",
      ]
        .map((column) => integer(`NEW.${column}`))
        .join(" AND ")}
      AND ${timestamp("NEW.terminal_at")}
      AND ${timestamp("NEW.finalized_at")}
      AND (NEW.evaluation_id IS NULL OR (${text("NEW.evaluation_id")}))
      AND (NEW.evaluation_evidence_id IS NULL OR (${text("NEW.evaluation_evidence_id")}))
      AND (NEW.evaluation_receipt_id IS NULL OR (${text("NEW.evaluation_receipt_id")}))
      AND (NEW.evaluation_marker_id IS NULL OR (${text("NEW.evaluation_marker_id")}))
      AND (NEW.evaluation_disposition IS NULL OR (${text("NEW.evaluation_disposition")}))
      AND (NEW.verification_verdict IS NULL OR (${text("NEW.verification_verdict")}))
      AND (NEW.invalid_output_code IS NULL OR (${text("NEW.invalid_output_code")}))
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification finalization evidence storage'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_verification_finalization_receipt_storage_validate
    BEFORE INSERT ON agent_control_verification_finalization_receipts
    WHEN NOT COALESCE((
      ${[
        "receipt_id",
        "marker_id",
        "finalization_evidence_id",
        "finalization_command_id",
        "handoff_id",
        "outcome",
        "terminal_cause",
        "stage_event_id",
        "lease_event_id",
        "status",
      ]
        .map((column) => text(`NEW.${column}`))
        .join(" AND ")}
      AND ${sha256("NEW.finalization_fingerprint")}
      AND ${integer("NEW.stage_event_sequence")}
      AND ${integer("NEW.lease_event_sequence")}
      AND ${timestamp("NEW.accepted_at")}
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification finalization receipt storage'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_verification_finalization_marker_storage_validate
    BEFORE INSERT ON agent_control_verification_finalization_markers
    WHEN NOT COALESCE((
      ${[
        "marker_id",
        "receipt_id",
        "finalization_evidence_id",
        "finalization_command_id",
        "handoff_id",
      ]
        .map((column) => text(`NEW.${column}`))
        .join(" AND ")}
      AND ${sha256("NEW.marker_fingerprint")}
      AND ${sha256("NEW.finalization_fingerprint")}
      AND ${timestamp("NEW.committed_at")}
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification finalization marker storage'); END
  `).unprepared;
  for (const table of [
    "agent_control_verification_finalization_evidence",
    "agent_control_verification_finalization_receipts",
    "agent_control_verification_finalization_markers",
  ] as const) {
    yield* sql.unsafe(`
      CREATE TRIGGER main.${table}_no_update BEFORE UPDATE ON ${table}
      BEGIN SELECT RAISE(ABORT, 'verification finalization authority is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.${table}_no_delete BEFORE DELETE ON ${table}
      BEGIN SELECT RAISE(ABORT, 'verification finalization authority is immutable'); END
    `).unprepared;
  }
});

const createEventValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_verification_terminal_stage_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.event_type IN (
      'agentControl.stageRun.verificationSucceeded',
      'agentControl.stageRun.verificationFailed',
      'agentControl.stageRun.verificationCancelled'
    ) AND NOT COALESCE((
      NEW.aggregate_kind = 'stage-run'
      AND NEW.stream_version = 3
      AND NEW.actor_authority = 'system'
      AND NEW.causation_event_id IS NOT NULL
      AND NEW.command_id = NEW.correlation_id
      AND json_extract(NEW.metadata_json, '$.schemaVersion') = 1
      AND json_extract(NEW.payload_json, '$.stageRunId') = NEW.stream_id
      AND json_extract(NEW.payload_json, '$.finalizedAt') = NEW.occurred_at
      AND json_extract(NEW.payload_json, '$.terminalRuntimeEventId') = NEW.causation_event_id
      AND json_extract(NEW.payload_json, '$.roleId') = 'verifier'
      AND json_extract(NEW.payload_json, '$.stageKind') = 'verification'
      AND json_extract(NEW.payload_json, '$.stageOrdinal') = 3
      AND json_extract(NEW.payload_json, '$.attemptOrdinal') = 1
      AND (
        (NEW.event_type = 'agentControl.stageRun.verificationSucceeded'
          AND json_extract(NEW.payload_json, '$.status') = 'succeeded'
          AND json_extract(NEW.payload_json, '$.deliveryTerminalState') = 'completed'
          AND json_extract(NEW.payload_json, '$.terminalCause') = 'verification-passed'
          AND json_extract(NEW.payload_json, '$.evaluation.evaluationAuthority') = 'accepted-evaluation'
          AND json_extract(NEW.payload_json, '$.evaluation.evaluationDisposition') = 'evaluated'
          AND json_extract(NEW.payload_json, '$.evaluation.verificationVerdict') = 'passed')
        OR (NEW.event_type = 'agentControl.stageRun.verificationFailed'
          AND json_extract(NEW.payload_json, '$.status') = 'failed'
          AND (
            (json_extract(NEW.payload_json, '$.deliveryTerminalState') = 'completed'
              AND json_extract(NEW.payload_json, '$.evaluation.evaluationAuthority') = 'accepted-evaluation'
              AND (
                (json_extract(NEW.payload_json, '$.terminalCause') = 'verification-failed'
                  AND json_extract(NEW.payload_json, '$.evaluation.evaluationDisposition') = 'evaluated'
                  AND json_extract(NEW.payload_json, '$.evaluation.verificationVerdict') = 'failed')
                OR (json_extract(NEW.payload_json, '$.terminalCause') = 'verification-invalid-output'
                  AND json_extract(NEW.payload_json, '$.evaluation.evaluationDisposition') = 'invalid-output'
                  AND json_type(NEW.payload_json, '$.evaluation.verificationVerdict') = 'null'
                  AND json_extract(NEW.payload_json, '$.evaluation.invalidOutputCode') IN (
                    'missing-final-message', 'output-too-large', 'invalid-utf8',
                    'malformed-json', 'unsupported-schema-version', 'schema-violation'))))
            OR (json_extract(NEW.payload_json, '$.deliveryTerminalState') = 'failed'
              AND json_extract(NEW.payload_json, '$.terminalCause') = 'provider-delivery-failed'
              AND json_extract(NEW.payload_json, '$.evaluation.evaluationAuthority') = 'not-applicable')
          ))
        OR (NEW.event_type = 'agentControl.stageRun.verificationCancelled'
          AND json_extract(NEW.payload_json, '$.status') = 'cancelled'
          AND json_extract(NEW.payload_json, '$.deliveryTerminalState') = 'interrupted'
          AND json_extract(NEW.payload_json, '$.terminalCause') = 'provider-delivery-interrupted'
          AND json_extract(NEW.payload_json, '$.evaluation.evaluationAuthority') = 'not-applicable')
      )
      AND EXISTS (
        SELECT 1
        FROM main.agent_control_verification_handoff_accepted accepted
        JOIN main.agent_control_verification_deliveries delivery
          ON delivery.handoff_id = accepted.handoff_id
        JOIN main.agent_control_verification_stage_started_evidence started
          ON started.handoff_id = accepted.handoff_id
        JOIN main.agent_control_verification_stage_started_receipts start_receipt
          ON start_receipt.start_evidence_id = started.start_evidence_id
        JOIN main.agent_control_verification_stage_started_markers start_marker
          ON start_marker.start_evidence_id = started.start_evidence_id
         AND start_marker.start_receipt_id = start_receipt.start_receipt_id
        WHERE accepted.handoff_id = json_extract(NEW.payload_json, '$.handoffId')
          AND accepted.handoff_fingerprint = json_extract(NEW.payload_json, '$.handoffFingerprint')
          AND delivery.provider_delivery_id = json_extract(NEW.payload_json, '$.providerDeliveryId')
          AND delivery.state = json_extract(NEW.payload_json, '$.deliveryTerminalState')
          AND delivery.revision = json_extract(NEW.payload_json, '$.deliveryRevision')
          AND delivery.terminal_event_id = NEW.causation_event_id
          AND delivery.terminal_at = NEW.occurred_at
          AND started.stage_run_id = NEW.stream_id
          AND started.start_evidence_id = json_extract(NEW.payload_json, '$.startEvidenceId')
          AND start_receipt.start_receipt_id = json_extract(NEW.payload_json, '$.startReceiptId')
          AND start_marker.start_marker_id = json_extract(NEW.payload_json, '$.startMarkerId')
      )
      AND (
        json_extract(NEW.payload_json, '$.deliveryTerminalState') != 'completed'
        OR EXISTS (
          SELECT 1
          FROM main.agent_control_verification_evaluation_evidence evidence
          JOIN main.agent_control_verification_evaluation_receipts receipt
            ON receipt.evaluation_id = evidence.evaluation_id
           AND receipt.evidence_id = evidence.evidence_id
          JOIN main.agent_control_verification_evaluation_markers marker
            ON marker.evaluation_id = evidence.evaluation_id
           AND marker.evidence_id = evidence.evidence_id
           AND marker.receipt_id = receipt.receipt_id
          WHERE evidence.evaluation_id = json_extract(NEW.payload_json, '$.evaluation.evaluationId')
            AND evidence.evidence_id = json_extract(NEW.payload_json, '$.evaluation.evaluationEvidenceId')
            AND receipt.receipt_id = json_extract(NEW.payload_json, '$.evaluation.evaluationReceiptId')
            AND marker.marker_id = json_extract(NEW.payload_json, '$.evaluation.evaluationMarkerId')
            AND receipt.status = 'accepted'
            AND evidence.disposition = json_extract(NEW.payload_json, '$.evaluation.evaluationDisposition')
            AND evidence.verdict IS json_extract(NEW.payload_json, '$.evaluation.verificationVerdict')
            AND evidence.error_code IS json_extract(NEW.payload_json, '$.evaluation.invalidOutputCode')
        )
      )
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification terminal stage event'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_verification_lease_release_event_validate
    BEFORE INSERT ON agent_control_events
    WHEN NEW.event_type = 'agentControl.stageRunLease.releasedAfterVerification'
      AND NOT COALESCE((
        NEW.aggregate_kind = 'stage-run-lease'
        AND NEW.actor_authority = 'system'
        AND NEW.causation_event_id IS NOT NULL
        AND NEW.command_id = NEW.correlation_id
        AND json_extract(NEW.payload_json, '$.leaseId') = NEW.stream_id
        AND json_extract(NEW.payload_json, '$.stageEventId') = NEW.causation_event_id
        AND json_extract(NEW.payload_json, '$.releasedAt') = NEW.occurred_at
        AND EXISTS (
          SELECT 1 FROM main.agent_control_events stage_event
          JOIN main.agent_control_stage_run_states stage
            ON stage.stage_run_id = stage_event.stream_id
          JOIN main.agent_control_stage_run_lease_states lease
            ON lease.lease_id = NEW.stream_id
          WHERE stage_event.event_id = NEW.causation_event_id
            AND stage_event.stream_version = 3
            AND stage_event.event_type IN (
              'agentControl.stageRun.verificationSucceeded',
              'agentControl.stageRun.verificationFailed',
              'agentControl.stageRun.verificationCancelled'
            )
            AND stage.status = json_extract(NEW.payload_json, '$.stageStatus')
            AND stage.revision = 3
            AND lease.status = 'reserved'
            AND lease.stage_run_id = stage.stage_run_id
            AND lease.holder_id = json_extract(NEW.payload_json, '$.holderId')
            AND lease.fence_token = json_extract(NEW.payload_json, '$.fenceToken')
            AND json_extract(stage_event.payload_json, '$.finalizationEvidenceId') =
              json_extract(NEW.payload_json, '$.finalizationEvidenceId')
            AND json_extract(stage_event.payload_json, '$.terminalCause') =
              json_extract(NEW.payload_json, '$.terminalCause')
            AND json_extract(stage_event.payload_json, '$.evaluation') =
              json_extract(NEW.payload_json, '$.evaluation')
        )
      ), 0)
    BEGIN SELECT RAISE(ABORT, 'invalid verification lease release event'); END
  `).unprepared;
  for (const operation of ["UPDATE", "DELETE"] as const) {
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_verification_finalization_event_no_${operation.toLowerCase()}
      BEFORE ${operation} ON agent_control_events
      WHEN OLD.event_type IN (
        'agentControl.stageRun.verificationSucceeded',
        'agentControl.stageRun.verificationFailed',
        'agentControl.stageRun.verificationCancelled',
        'agentControl.stageRunLease.releasedAfterVerification'
      )
      BEGIN SELECT RAISE(ABORT, 'verification finalization events are immutable'); END
    `).unprepared;
  }
});

const createCompanionValidation = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_verification_finalization_evidence_validate
    BEFORE INSERT ON agent_control_verification_finalization_evidence
    WHEN NOT COALESCE((
      json_extract(NEW.finalization_json, '$.schemaVersion') = 1
      AND json_extract(NEW.finalization_json, '$.handoffId') = NEW.handoff_id
      AND json_extract(NEW.finalization_json, '$.handoffFingerprint') = NEW.handoff_fingerprint
      AND json_extract(NEW.finalization_json, '$.finalizationCommandId') = NEW.finalization_command_id
      AND json_extract(NEW.finalization_json, '$.finalizationEvidenceId') = NEW.finalization_evidence_id
      AND json_extract(NEW.finalization_json, '$.outcome') = NEW.outcome
      AND json_extract(NEW.finalization_json, '$.terminalCause') = NEW.terminal_cause
      AND json_extract(NEW.finalization_json, '$.deliveryTerminalState') = NEW.delivery_terminal_state
      AND json_extract(NEW.finalization_json, '$.terminalRuntimeEventId') = NEW.terminal_runtime_event_id
      AND json_extract(NEW.finalization_json, '$.stageEventId') = NEW.stage_event_id
      AND json_extract(NEW.finalization_json, '$.stageEventSequence') = NEW.stage_event_sequence
      AND json_extract(NEW.finalization_json, '$.leaseEventId') = NEW.lease_event_id
      AND json_extract(NEW.finalization_json, '$.leaseEventSequence') = NEW.lease_event_sequence
      AND json_extract(NEW.finalization_json, '$.finalizedAt') = NEW.finalized_at
      AND EXISTS (
        SELECT 1
        FROM main.agent_control_verification_handoff_accepted accepted
        JOIN main.agent_control_verification_deliveries delivery
          ON delivery.handoff_id = accepted.handoff_id
        JOIN main.agent_control_verification_stage_started_evidence started
          ON started.handoff_id = accepted.handoff_id
        JOIN main.agent_control_verification_stage_started_receipts start_receipt
          ON start_receipt.start_evidence_id = started.start_evidence_id
        JOIN main.agent_control_verification_stage_started_markers start_marker
          ON start_marker.start_evidence_id = started.start_evidence_id
         AND start_marker.start_receipt_id = start_receipt.start_receipt_id
        JOIN main.agent_control_events stage_event ON stage_event.event_id = NEW.stage_event_id
        JOIN main.agent_control_events lease_event ON lease_event.event_id = NEW.lease_event_id
        JOIN main.agent_control_stage_run_states stage ON stage.stage_run_id = NEW.stage_run_id
        JOIN main.agent_control_stage_run_lease_states lease ON lease.lease_id = NEW.lease_id
        WHERE accepted.handoff_id = NEW.handoff_id
          AND accepted.handoff_fingerprint = NEW.handoff_fingerprint
          AND delivery.provider_delivery_id = NEW.provider_delivery_id
          AND delivery.provider_instance_id = NEW.provider_instance_id
          AND delivery.provider_turn_id = NEW.provider_turn_id
          AND delivery.revision = NEW.delivery_revision
          AND delivery.state = NEW.delivery_terminal_state
          AND delivery.terminal_event_id = NEW.terminal_runtime_event_id
          AND delivery.terminal_at = NEW.terminal_at
          AND delivery.terminal_at = NEW.finalized_at
          AND started.start_evidence_id = NEW.start_evidence_id
          AND start_receipt.start_receipt_id = NEW.start_receipt_id
          AND start_marker.start_marker_id = NEW.start_marker_id
          AND started.stage_run_id = NEW.stage_run_id
          AND started.attempt_id = NEW.attempt_id
          AND started.lease_id = NEW.lease_id
          AND started.lease_holder_id = NEW.lease_holder_id
          AND started.fence_token = NEW.fence_token
          AND stage_event.stream_id = NEW.stage_run_id
          AND stage_event.stream_version = NEW.stage_event_stream_version
          AND stage_event.sequence = NEW.stage_event_sequence
          AND json_extract(stage_event.payload_json, '$.finalizationEvidenceId') = NEW.finalization_evidence_id
          AND lease_event.stream_id = NEW.lease_id
          AND lease_event.stream_version = NEW.lease_event_stream_version
          AND lease_event.sequence = NEW.lease_event_sequence
          AND lease_event.event_type = 'agentControl.stageRunLease.releasedAfterVerification'
          AND json_extract(lease_event.payload_json, '$.finalizationEvidenceId') = NEW.finalization_evidence_id
          AND stage.status = NEW.outcome AND stage.revision = 3
          AND stage.last_event_sequence = NEW.stage_event_sequence
          AND lease.status = 'released'
          AND lease.last_event_sequence = NEW.lease_event_sequence
          AND lease.holder_id = NEW.lease_holder_id
          AND lease.fence_token = NEW.fence_token
      )
      AND (
        NEW.evaluation_authority = 'not-applicable'
        OR EXISTS (
          SELECT 1
          FROM main.agent_control_verification_evaluation_evidence evaluation
          JOIN main.agent_control_verification_evaluation_receipts evaluation_receipt
            ON evaluation_receipt.evaluation_id = evaluation.evaluation_id
           AND evaluation_receipt.evidence_id = evaluation.evidence_id
          JOIN main.agent_control_verification_evaluation_markers evaluation_marker
            ON evaluation_marker.evaluation_id = evaluation.evaluation_id
           AND evaluation_marker.evidence_id = evaluation.evidence_id
           AND evaluation_marker.receipt_id = evaluation_receipt.receipt_id
          WHERE evaluation.evaluation_id = NEW.evaluation_id
            AND evaluation.evidence_id = NEW.evaluation_evidence_id
            AND evaluation_receipt.receipt_id = NEW.evaluation_receipt_id
            AND evaluation_marker.marker_id = NEW.evaluation_marker_id
            AND evaluation.provider_delivery_id = NEW.provider_delivery_id
            AND evaluation.disposition = NEW.evaluation_disposition
            AND evaluation.verdict IS NEW.verification_verdict
            AND evaluation.error_code IS NEW.invalid_output_code
            AND evaluation_receipt.status = 'accepted'
        )
      )
    ), 0)
    BEGIN SELECT RAISE(ABORT, 'verification finalization evidence is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_verification_finalization_receipt_validate
    BEFORE INSERT ON agent_control_verification_finalization_receipts
    WHEN NOT COALESCE((EXISTS (
      SELECT 1 FROM main.agent_control_verification_finalization_evidence evidence
      WHERE evidence.finalization_evidence_id = NEW.finalization_evidence_id
        AND evidence.receipt_id = NEW.receipt_id
        AND evidence.marker_id = NEW.marker_id
        AND evidence.finalization_command_id = NEW.finalization_command_id
        AND evidence.finalization_fingerprint = NEW.finalization_fingerprint
        AND evidence.handoff_id = NEW.handoff_id
        AND evidence.outcome = NEW.outcome
        AND evidence.terminal_cause = NEW.terminal_cause
        AND evidence.stage_event_id = NEW.stage_event_id
        AND evidence.stage_event_sequence = NEW.stage_event_sequence
        AND evidence.lease_event_id = NEW.lease_event_id
        AND evidence.lease_event_sequence = NEW.lease_event_sequence
        AND evidence.finalized_at = NEW.accepted_at
        AND NEW.status = 'accepted'
    )), 0)
    BEGIN SELECT RAISE(ABORT, 'verification finalization receipt is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_verification_finalization_marker_validate
    BEFORE INSERT ON agent_control_verification_finalization_markers
    WHEN NOT COALESCE((EXISTS (
      SELECT 1
      FROM main.agent_control_verification_finalization_evidence evidence
      JOIN main.agent_control_verification_finalization_receipts receipt
        ON receipt.finalization_evidence_id = evidence.finalization_evidence_id
      WHERE evidence.marker_id = NEW.marker_id
        AND evidence.receipt_id = NEW.receipt_id
        AND receipt.receipt_id = NEW.receipt_id
        AND evidence.finalization_evidence_id = NEW.finalization_evidence_id
        AND evidence.finalization_command_id = NEW.finalization_command_id
        AND receipt.finalization_command_id = NEW.finalization_command_id
        AND evidence.finalization_fingerprint = NEW.finalization_fingerprint
        AND receipt.finalization_fingerprint = NEW.finalization_fingerprint
        AND evidence.handoff_id = NEW.handoff_id
        AND receipt.handoff_id = NEW.handoff_id
        AND evidence.finalized_at = NEW.committed_at
        AND receipt.accepted_at = NEW.committed_at
        AND receipt.status = 'accepted'
    )), 0)
    BEGIN SELECT RAISE(ABORT, 'verification finalization marker is inconsistent'); END
  `).unprepared;
});

/** Durable terminal Verification Stage authority and exact-once lease release. */
export const makeMigration061 = (faultPoint?: Migration061FaultPoint) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const injectFault = (point: Migration061FaultPoint) =>
      faultPoint === point
        ? Effect.die(new Error(`migration 061 injected ${point} failure`))
        : Effect.void;
    const existing = yield* sql<{ readonly count: number }>`
      SELECT count(*) AS count FROM main.sqlite_schema
      WHERE type = 'table' AND name = 'agent_control_verification_finalization_markers'
    `;
    if (existing[0]?.count === 1) return;

    yield* sql`PRAGMA defer_foreign_keys = ON`;
    yield* injectFault("before-events-rebuild");
    yield* rebuildAgentControlEvents;
    yield* injectFault("after-events-rebuild");
    yield* createCompanions;
    yield* injectFault("after-companions");
    yield* createStorageAndImmutability;
    yield* createEventValidation;
    yield* createCompanionValidation;
    yield* injectFault("after-install");
    const foreignKeyViolations = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
    if (foreignKeyViolations.length !== 0) {
      return yield* Effect.die(new Error("migration 061 introduced foreign-key violations"));
    }
    const integrity = yield* sql<{ readonly integrity_check: string }>`PRAGMA integrity_check`;
    if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") {
      return yield* Effect.die(new Error("migration 061 failed integrity_check"));
    }
    yield* sql`PRAGMA defer_foreign_keys = OFF`;
  });

export default makeMigration061();
