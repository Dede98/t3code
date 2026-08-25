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
const text = (column: string) =>
  `typeof(${column}) = 'text' AND length(${column}) > 0 AND ${canonicalUtf8(column)}`;
const nullableText = (column: string) => `(${column} IS NULL OR (${text(column)}))`;
const integer = (column: string) => `typeof(${column}) = 'integer'`;
const nullableInteger = (column: string) => `(${column} IS NULL OR typeof(${column}) = 'integer')`;
const sha256 = (column: string) =>
  `${text(column)} AND length(${column}) = 64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;
const nullableSha256 = (column: string) => `(${column} IS NULL OR (${sha256(column)}))`;
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

const resultContractPredicate = (row = "NEW") => `
  (
    ${row}.prompt_template_version IS NULL
    AND ${row}.prompt_contract_fingerprint IS NULL
    AND ${row}.result_schema_version IS NULL
    AND ${row}.result_schema_fingerprint IS NULL
  ) OR (
    ${row}.template_version = 'agent-control-verification-prompt-v1'
    AND ${row}.prompt_template_version = 'agent-control-verification-prompt-v2'
    AND ${sha256(`${row}.prompt_contract_fingerprint`)}
    AND ${row}.result_schema_version = 'agent-control-verification-result-v1'
    AND ${sha256(`${row}.result_schema_fingerprint`)}
  )
`;

const evidenceStorage = (row = "NEW") =>
  [
    ...[
      "evaluation_id",
      "evidence_id",
      "disposition",
      "source_disposition",
      "project_id",
      "task_id",
      "source_identity_fingerprint",
      "worktree_reservation_id",
      "worktree_event_id",
      "worktree_ownership_fingerprint",
      "worktree_verified_at",
      "worktree_path",
      "branch",
      "stage_run_id",
      "attempt_id",
      "lease_id",
      "lease_holder_id",
      "controlled_thread_reservation_id",
      "handoff_id",
      "provider_delivery_id",
      "thread_id",
      "provider_instance_id",
      "provider_turn_id",
      "prompt_template_version",
      "result_schema_version",
      "terminal_event_id",
      "terminal_state",
      "start_marker_id",
      "receipt_id",
      "marker_id",
    ].map((column) => text(`${row}.${column}`)),
    ...[
      "evaluation_fingerprint",
      "authority_digest",
      "handoff_fingerprint",
      "model_selection_fingerprint",
      "prompt_contract_fingerprint",
      "prompt_digest",
      "result_schema_fingerprint",
      "terminal_observation_digest",
    ].map((column) => sha256(`${row}.${column}`)),
    ...["verdict", "error_code", "source_message_id", "source_event_id"].map((column) =>
      nullableText(`${row}.${column}`),
    ),
    ...["raw_output_digest", "semantic_result_digest"].map((column) =>
      nullableSha256(`${row}.${column}`),
    ),
    ...[
      "revision",
      "task_revision",
      "github_intake_sequence",
      "worktree_revision",
      "worktree_event_sequence",
      "worktree_event_stream_version",
      "fence_token",
      "terminal_sequence",
      "terminal_stream_version",
      "output_byte_length",
    ].map((column) => integer(`${row}.${column}`)),
    ...["source_event_sequence", "source_event_stream_version"].map((column) =>
      nullableInteger(`${row}.${column}`),
    ),
    canonicalJson(`${row}.authority_json`),
    timestamp(`${row}.evaluated_at`),
  ].join(" AND ");

const receiptStorage = (row = "NEW") =>
  [
    ...[
      "receipt_id",
      "evaluation_id",
      "evidence_id",
      "marker_id",
      "provider_delivery_id",
      "provider_instance_id",
      "provider_turn_id",
      "terminal_event_id",
      "disposition",
      "source_disposition",
      "status",
    ].map((column) => text(`${row}.${column}`)),
    ...["evaluation_fingerprint", "terminal_observation_digest"].map((column) =>
      sha256(`${row}.${column}`),
    ),
    ...["source_message_id", "source_event_id", "verdict", "error_code"].map((column) =>
      nullableText(`${row}.${column}`),
    ),
    nullableSha256(`${row}.raw_output_digest`),
    integer(`${row}.output_byte_length`),
    timestamp(`${row}.accepted_at`),
  ].join(" AND ");

const markerStorage = (row = "NEW") =>
  [
    ...["marker_id", "evaluation_id", "evidence_id", "receipt_id", "provider_delivery_id"].map(
      (column) => text(`${row}.${column}`),
    ),
    sha256(`${row}.evaluation_fingerprint`),
    integer(`${row}.marker_version`),
    timestamp(`${row}.committed_at`),
  ].join(" AND ");

export type Migration060FaultPoint = "before-copy" | "after-copy" | "after-install";

export const makeMigration060 = (faultPoint?: Migration060FaultPoint) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const injectFault = (point: Migration060FaultPoint) =>
      faultPoint === point
        ? Effect.die(new Error(`migration 060 injected ${point} failure`))
        : Effect.void;

    const contractColumns = yield* sql<{ readonly name: string }>`
      SELECT name FROM pragma_table_info('agent_control_verification_handoff_intents')
      WHERE name = 'prompt_template_version'
    `;
    if (contractColumns.length === 0) {
      const triggerRows = yield* sql<{ readonly sql: string }>`
        SELECT sql FROM sqlite_schema
        WHERE type = 'trigger' AND name = 'agent_control_verification_handoff_intent_validate'
          AND sql IS NOT NULL
      `;
      if (triggerRows.length !== 1) {
        return yield* Effect.die(new Error("migration 060 could not capture handoff validation"));
      }
      const originalTrigger = triggerRows[0]!.sql;
      const expandedTrigger = originalTrigger.replace(
        "AND NEW.template_version IS 'agent-control-verification-prompt-v1'",
        `AND NEW.template_version IS 'agent-control-verification-prompt-v1'
        AND (${resultContractPredicate()})`,
      );
      if (expandedTrigger === originalTrigger) {
        return yield* Effect.die(new Error("migration 060 could not expand handoff validation"));
      }

      yield* injectFault("before-copy");

      yield* sql`
        ALTER TABLE agent_control_verification_handoff_intents
        ADD COLUMN prompt_template_version TEXT CHECK (
          prompt_template_version IS NULL
          OR prompt_template_version = 'agent-control-verification-prompt-v2'
        )
      `;
      yield* sql`
        ALTER TABLE agent_control_verification_handoff_intents
        ADD COLUMN prompt_contract_fingerprint TEXT
      `;
      yield* sql`
        ALTER TABLE agent_control_verification_handoff_intents
        ADD COLUMN result_schema_version TEXT
      `;
      yield* sql`
        ALTER TABLE agent_control_verification_handoff_intents
        ADD COLUMN result_schema_fingerprint TEXT
      `;

      yield* sql`DROP TRIGGER agent_control_verification_handoff_intent_validate`;
      yield* sql.unsafe(expandedTrigger).unprepared;
      yield* sql.unsafe(`
        CREATE TRIGGER agent_control_verification_handoff_result_contract_storage_validate
        BEFORE INSERT ON agent_control_verification_handoff_intents
        WHEN NOT COALESCE((${resultContractPredicate()}), 0)
        BEGIN SELECT RAISE(ABORT, 'invalid verification result contract storage'); END
      `).unprepared;
      yield* sql.unsafe(`
        CREATE TRIGGER agent_control_verification_handoff_result_contract_update_storage_validate
        BEFORE UPDATE ON agent_control_verification_handoff_intents
        WHEN NOT COALESCE((${resultContractPredicate()}), 0)
        BEGIN SELECT RAISE(ABORT, 'invalid verification result contract storage'); END
      `).unprepared;
    }
    yield* injectFault("after-copy");

    yield* sql`
      CREATE TABLE agent_control_verification_evaluation_evidence (
        evaluation_id TEXT PRIMARY KEY,
        evidence_id TEXT NOT NULL UNIQUE,
        revision INTEGER NOT NULL CHECK (revision = 1),
        evaluation_fingerprint TEXT NOT NULL UNIQUE,
        authority_digest TEXT NOT NULL,
        authority_json TEXT NOT NULL CHECK (json_valid(authority_json) = 1),
        disposition TEXT NOT NULL CHECK (disposition IN ('evaluated', 'invalid-output')),
        verdict TEXT CHECK (verdict IS NULL OR verdict IN ('passed', 'failed')),
        error_code TEXT CHECK (error_code IS NULL OR error_code IN (
          'missing-final-message', 'output-too-large', 'invalid-utf8',
          'malformed-json', 'unsupported-schema-version', 'schema-violation'
        )),
        source_disposition TEXT NOT NULL CHECK (
          source_disposition IN ('captured', 'missing', 'oversize')
        ),
        project_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        task_revision INTEGER NOT NULL CHECK (task_revision >= 1),
        github_intake_sequence INTEGER NOT NULL CHECK (github_intake_sequence >= 1),
        source_identity_fingerprint TEXT NOT NULL,
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
        stage_run_id TEXT NOT NULL UNIQUE,
        attempt_id TEXT NOT NULL UNIQUE,
        lease_id TEXT NOT NULL,
        lease_holder_id TEXT NOT NULL,
        fence_token INTEGER NOT NULL CHECK (fence_token >= 3),
        controlled_thread_reservation_id TEXT NOT NULL UNIQUE,
        handoff_id TEXT NOT NULL UNIQUE,
        handoff_fingerprint TEXT NOT NULL UNIQUE,
        provider_delivery_id TEXT NOT NULL UNIQUE,
        thread_id TEXT NOT NULL UNIQUE,
        provider_instance_id TEXT NOT NULL,
        provider_turn_id TEXT NOT NULL,
        model_selection_fingerprint TEXT NOT NULL,
        prompt_template_version TEXT NOT NULL CHECK (
          prompt_template_version = 'agent-control-verification-prompt-v2'
        ),
        prompt_contract_fingerprint TEXT NOT NULL,
        prompt_digest TEXT NOT NULL,
        result_schema_version TEXT NOT NULL CHECK (
          result_schema_version = 'agent-control-verification-result-v1'
        ),
        result_schema_fingerprint TEXT NOT NULL,
        terminal_event_id TEXT NOT NULL UNIQUE,
        terminal_sequence INTEGER NOT NULL UNIQUE CHECK (terminal_sequence >= 1),
        terminal_stream_version INTEGER NOT NULL CHECK (terminal_stream_version >= 1),
        terminal_state TEXT NOT NULL CHECK (terminal_state = 'completed'),
        terminal_observation_digest TEXT NOT NULL,
        source_message_id TEXT,
        source_event_id TEXT,
        source_event_sequence INTEGER,
        source_event_stream_version INTEGER,
        raw_output_digest TEXT,
        output_byte_length INTEGER NOT NULL CHECK (output_byte_length >= 0),
        semantic_result_digest TEXT,
        start_marker_id TEXT NOT NULL UNIQUE,
        evaluated_at TEXT NOT NULL,
        receipt_id TEXT NOT NULL UNIQUE,
        marker_id TEXT NOT NULL UNIQUE,
        CHECK (
          (source_disposition = 'missing' AND source_message_id IS NULL
            AND source_event_id IS NULL AND source_event_sequence IS NULL
            AND source_event_stream_version IS NULL AND raw_output_digest IS NULL
            AND output_byte_length = 0)
          OR (source_disposition IN ('captured', 'oversize')
            AND source_message_id IS NOT NULL AND source_event_id IS NOT NULL
            AND source_event_sequence IS NOT NULL AND source_event_stream_version IS NOT NULL
            AND raw_output_digest IS NOT NULL)
        ),
        CHECK (
          (disposition = 'evaluated' AND verdict IS NOT NULL AND error_code IS NULL
            AND source_disposition = 'captured' AND semantic_result_digest IS NOT NULL)
          OR (disposition = 'invalid-output' AND verdict IS NULL AND error_code IS NOT NULL
            AND semantic_result_digest IS NULL)
        ),
        FOREIGN KEY (handoff_id)
          REFERENCES agent_control_verification_handoff_accepted(handoff_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (provider_delivery_id)
          REFERENCES agent_control_verification_deliveries(provider_delivery_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (start_marker_id)
          REFERENCES agent_control_verification_stage_started_markers(start_marker_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (terminal_event_id)
          REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (source_event_id)
          REFERENCES orchestration_events(event_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (receipt_id)
          REFERENCES agent_control_verification_evaluation_receipts(receipt_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
        FOREIGN KEY (marker_id)
          REFERENCES agent_control_verification_evaluation_markers(marker_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
      )
    `;
    yield* sql`
      CREATE TABLE agent_control_verification_evaluation_receipts (
        receipt_id TEXT PRIMARY KEY,
        evaluation_id TEXT NOT NULL UNIQUE,
        evidence_id TEXT NOT NULL UNIQUE,
        marker_id TEXT NOT NULL UNIQUE,
        evaluation_fingerprint TEXT NOT NULL UNIQUE,
        provider_delivery_id TEXT NOT NULL UNIQUE,
        provider_instance_id TEXT NOT NULL,
        provider_turn_id TEXT NOT NULL,
        terminal_event_id TEXT NOT NULL UNIQUE,
        terminal_observation_digest TEXT NOT NULL,
        source_message_id TEXT,
        source_event_id TEXT,
        raw_output_digest TEXT,
        output_byte_length INTEGER NOT NULL CHECK (output_byte_length >= 0),
        source_disposition TEXT NOT NULL CHECK (
          source_disposition IN ('captured', 'missing', 'oversize')
        ),
        disposition TEXT NOT NULL CHECK (disposition IN ('evaluated', 'invalid-output')),
        verdict TEXT CHECK (verdict IS NULL OR verdict IN ('passed', 'failed')),
        error_code TEXT CHECK (error_code IS NULL OR error_code IN (
          'missing-final-message', 'output-too-large', 'invalid-utf8',
          'malformed-json', 'unsupported-schema-version', 'schema-violation'
        )),
        status TEXT NOT NULL CHECK (status = 'accepted'),
        accepted_at TEXT NOT NULL,
        FOREIGN KEY (evaluation_id)
          REFERENCES agent_control_verification_evaluation_evidence(evaluation_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (evidence_id)
          REFERENCES agent_control_verification_evaluation_evidence(evidence_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (marker_id)
          REFERENCES agent_control_verification_evaluation_markers(marker_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
      )
    `;
    yield* sql`
      CREATE TABLE agent_control_verification_evaluation_markers (
        marker_id TEXT PRIMARY KEY,
        evaluation_id TEXT NOT NULL UNIQUE,
        evidence_id TEXT NOT NULL UNIQUE,
        receipt_id TEXT NOT NULL UNIQUE,
        evaluation_fingerprint TEXT NOT NULL UNIQUE,
        provider_delivery_id TEXT NOT NULL UNIQUE,
        marker_version INTEGER NOT NULL CHECK (marker_version = 1),
        committed_at TEXT NOT NULL,
        FOREIGN KEY (evaluation_id)
          REFERENCES agent_control_verification_evaluation_evidence(evaluation_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (evidence_id)
          REFERENCES agent_control_verification_evaluation_evidence(evidence_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        FOREIGN KEY (receipt_id)
          REFERENCES agent_control_verification_evaluation_receipts(receipt_id)
          ON UPDATE RESTRICT ON DELETE RESTRICT
      )
    `;
    yield* sql`
      CREATE UNIQUE INDEX idx_agent_control_verification_evaluation_provider_turn
      ON agent_control_verification_evaluation_evidence(provider_instance_id, provider_turn_id)
    `;
    yield* sql`
      CREATE INDEX idx_agent_control_verification_evaluation_candidate
      ON agent_control_verification_handoff_intents(prompt_template_version, handoff_id)
      WHERE prompt_template_version = 'agent-control-verification-prompt-v2'
    `;
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_evaluation_evidence_storage_validate
      BEFORE INSERT ON agent_control_verification_evaluation_evidence
      WHEN NOT COALESCE((
        ${evidenceStorage()}
        AND json_type(NEW.authority_json, '$.report') IS NULL
      ), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification evaluation evidence storage'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_evaluation_receipt_storage_validate
      BEFORE INSERT ON agent_control_verification_evaluation_receipts
      WHEN NOT COALESCE((${receiptStorage()}), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification evaluation receipt storage'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_evaluation_marker_storage_validate
      BEFORE INSERT ON agent_control_verification_evaluation_markers
      WHEN NOT COALESCE((${markerStorage()}), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification evaluation marker storage'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_result_capture_validate
      BEFORE INSERT ON orchestration_events
      WHEN NEW.event_type = 'thread.verification-result-fragment-captured'
        AND NOT COALESCE(EXISTS (
          SELECT 1
          FROM agent_control_verification_deliveries delivery
          JOIN agent_control_verification_handoff_intents intent
            ON intent.handoff_id = delivery.handoff_id
          WHERE typeof(NEW.stream_id) = 'text'
            AND typeof(NEW.event_type) = 'text'
            AND typeof(NEW.command_id) = 'text'
            AND typeof(NEW.actor_kind) = 'text'
            AND typeof(NEW.payload_json) = 'text'
            AND typeof(NEW.metadata_json) = 'text'
            AND json_valid(NEW.payload_json) = 1
            AND json_valid(NEW.metadata_json) = 1
            AND NEW.actor_kind = 'provider'
            AND json_extract(NEW.payload_json, '$.threadId') IS NEW.stream_id
            AND json_extract(NEW.payload_json, '$.turnId') IS delivery.provider_turn_id
            AND json_type(NEW.payload_json, '$.messageId') = 'text'
            AND (
              (json_extract(NEW.payload_json, '$.fragment.kind') = 'delta'
                AND json_type(NEW.payload_json, '$.fragment.text') = 'text'
                AND json_extract(
                  NEW.metadata_json, '$.providerRuntimeMessage.runtimeEventType'
                ) = 'content.delta')
              OR
              (json_extract(NEW.payload_json, '$.fragment.kind') = 'completion'
                AND json_type(NEW.payload_json, '$.fragment.text') IS NULL
                AND json_extract(
                  NEW.metadata_json, '$.providerRuntimeMessage.runtimeEventType'
                ) IN ('item.completed', 'request.opened', 'user-input.requested', 'turn.completed'))
            )
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.schemaVersion'
            ) = 1
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.disposition'
            ) = 'authority'
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.handoffId'
            ) IS delivery.handoff_id
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
            ) IS delivery.provider_delivery_id
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.providerInstanceId'
            ) IS delivery.provider_instance_id
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.providerTurnId'
            ) IS delivery.provider_turn_id
            AND json_extract(
              NEW.metadata_json, '$.verificationResultCapture.resultSchemaFingerprint'
            ) IS intent.result_schema_fingerprint
            AND json_extract(
              NEW.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
            ) IS delivery.provider_instance_id
            AND json_extract(
              NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId'
            ) IS delivery.provider_turn_id
            AND NEW.command_id LIKE 'provider:' || json_extract(
              NEW.metadata_json, '$.providerRuntimeMessage.runtimeEventId'
            ) || ':%'
            AND intent.prompt_template_version = 'agent-control-verification-prompt-v2'
            AND delivery.thread_id IS NEW.stream_id
            AND delivery.state IN ('provider-started', 'completed')
        ), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid verification result capture authority'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_result_post_seal_reject
      BEFORE INSERT ON orchestration_events
      WHEN NEW.event_type IN (
        'thread.message-sent', 'thread.verification-result-fragment-captured'
      ) AND COALESCE(EXISTS (
        SELECT 1
        FROM orchestration_events sealed
        WHERE sealed.stream_id IS NEW.stream_id
          AND sealed.event_type = 'thread.session-set'
          AND json_type(sealed.metadata_json, '$.verificationResultSource') = 'object'
          AND (
            (
              NEW.event_type = 'thread.verification-result-fragment-captured'
              AND json_extract(
                NEW.metadata_json, '$.verificationResultCapture.handoffId'
              ) IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.handoffId'
              )
              AND json_extract(
                NEW.metadata_json, '$.verificationResultCapture.providerDeliveryId'
              ) IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.providerDeliveryId'
              )
              AND json_extract(
                NEW.metadata_json, '$.verificationResultCapture.resultSchemaFingerprint'
              ) IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.resultSchemaFingerprint'
              )
              AND json_extract(
                NEW.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
              ) IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.providerInstanceId'
              )
              AND json_extract(
                NEW.metadata_json, '$.providerRuntimeMessage.providerTurnId'
              ) IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.providerTurnId'
              )
            )
            OR (
              NEW.event_type = 'thread.message-sent'
              AND json_extract(NEW.payload_json, '$.role') = 'assistant'
              AND json_extract(NEW.payload_json, '$.turnId') IS json_extract(
                sealed.metadata_json, '$.verificationResultSource.providerTurnId'
              )
            )
          )
      ), 0)
      BEGIN SELECT RAISE(ABORT, 'verification result source is sealed'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_result_authority_no_update
      BEFORE UPDATE ON orchestration_events
      WHEN json_type(OLD.metadata_json, '$.verificationResultCapture') = 'object'
        OR json_type(OLD.metadata_json, '$.verificationResultSource') = 'object'
        OR json_type(NEW.metadata_json, '$.verificationResultCapture') = 'object'
        OR json_type(NEW.metadata_json, '$.verificationResultSource') = 'object'
        OR EXISTS (
          SELECT 1 FROM orchestration_events sealed
          WHERE json_type(sealed.metadata_json, '$.verificationResultSource') = 'object'
            AND (
              json_extract(sealed.metadata_json, '$.verificationResultSource.sourceEventId')
                IS OLD.event_id
              OR json_extract(sealed.metadata_json, '$.verificationResultSource.sourceEventId')
                IS NEW.event_id
            )
        )
      BEGIN SELECT RAISE(ABORT, 'verification result authority is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_result_authority_no_delete
      BEFORE DELETE ON orchestration_events
      WHEN json_type(OLD.metadata_json, '$.verificationResultCapture') = 'object'
        OR json_type(OLD.metadata_json, '$.verificationResultSource') = 'object'
        OR EXISTS (
          SELECT 1 FROM orchestration_events sealed
          WHERE json_type(sealed.metadata_json, '$.verificationResultSource') = 'object'
            AND json_extract(
              sealed.metadata_json, '$.verificationResultSource.sourceEventId'
            ) IS OLD.event_id
        )
      BEGIN SELECT RAISE(ABORT, 'verification result authority is immutable'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_evaluation_evidence_validate
      BEFORE INSERT ON agent_control_verification_evaluation_evidence
      WHEN NOT EXISTS (
        SELECT 1
        FROM agent_control_verification_deliveries delivery
        JOIN agent_control_verification_handoff_intents intent
          ON intent.handoff_id = delivery.handoff_id
        JOIN agent_control_verification_stage_started_markers started
          ON started.provider_delivery_id = delivery.provider_delivery_id
        JOIN agent_control_verification_stage_started_evidence stage_start
          ON stage_start.start_evidence_id = started.start_evidence_id
        JOIN agent_control_stage_run_states stage
          ON stage.stage_run_id = delivery.stage_run_id
        JOIN agent_control_stage_run_lease_states lease
          ON lease.lease_id = delivery.lease_id
        JOIN orchestration_events terminal
          ON terminal.event_id = NEW.terminal_event_id
        LEFT JOIN orchestration_events source
          ON source.event_id = NEW.source_event_id
        WHERE delivery.provider_delivery_id IS NEW.provider_delivery_id
          AND delivery.handoff_id IS NEW.handoff_id
          AND delivery.handoff_fingerprint IS NEW.handoff_fingerprint
          AND delivery.thread_id IS NEW.thread_id
          AND delivery.stage_run_id IS NEW.stage_run_id
          AND delivery.attempt_id IS NEW.attempt_id
          AND delivery.lease_id IS NEW.lease_id
          AND delivery.lease_holder_id IS NEW.lease_holder_id
          AND delivery.fence_token IS NEW.fence_token
          AND delivery.controlled_thread_reservation_id IS
            NEW.controlled_thread_reservation_id
          AND delivery.provider_instance_id IS NEW.provider_instance_id
          AND delivery.provider_turn_id IS NEW.provider_turn_id
          AND delivery.model_selection_fingerprint IS NEW.model_selection_fingerprint
          AND delivery.state = 'completed'
          AND delivery.terminal_provider_state = 'completed'
          AND delivery.terminal_observation_digest IS NEW.terminal_observation_digest
          AND intent.project_id IS NEW.project_id
          AND intent.task_id IS NEW.task_id
          AND intent.task_revision IS NEW.task_revision
          AND intent.github_intake_sequence IS NEW.github_intake_sequence
          AND intent.source_identity_fingerprint IS NEW.source_identity_fingerprint
          AND intent.worktree_reservation_id IS NEW.worktree_reservation_id
          AND intent.worktree_revision IS NEW.worktree_revision
          AND intent.worktree_event_id IS NEW.worktree_event_id
          AND intent.worktree_event_sequence IS NEW.worktree_event_sequence
          AND intent.worktree_event_stream_version IS NEW.worktree_event_stream_version
          AND intent.worktree_ownership_fingerprint IS NEW.worktree_ownership_fingerprint
          AND intent.worktree_verified_at IS NEW.worktree_verified_at
          AND intent.worktree_path IS NEW.worktree_path
          AND intent.branch IS NEW.branch
          AND stage_start.handoff_id IS NEW.handoff_id
          AND stage_start.handoff_fingerprint IS NEW.handoff_fingerprint
          AND stage_start.project_id IS NEW.project_id
          AND stage_start.task_id IS NEW.task_id
          AND stage_start.task_revision IS NEW.task_revision
          AND stage_start.github_intake_sequence IS NEW.github_intake_sequence
          AND stage_start.source_identity_fingerprint IS NEW.source_identity_fingerprint
          AND stage_start.stage_run_id IS NEW.stage_run_id
          AND stage_start.attempt_id IS NEW.attempt_id
          AND stage_start.controlled_thread_reservation_id IS
            NEW.controlled_thread_reservation_id
          AND stage_start.thread_id IS NEW.thread_id
          AND stage_start.lease_id IS NEW.lease_id
          AND stage_start.lease_holder_id IS NEW.lease_holder_id
          AND stage_start.fence_token IS NEW.fence_token
          AND stage_start.provider_instance_id IS NEW.provider_instance_id
          AND stage_start.provider_turn_id IS NEW.provider_turn_id
          AND stage_start.model_selection_fingerprint IS NEW.model_selection_fingerprint
          AND intent.prompt_template_version IS NEW.prompt_template_version
          AND intent.prompt_contract_fingerprint IS NEW.prompt_contract_fingerprint
          AND intent.prompt_digest IS NEW.prompt_digest
          AND intent.result_schema_version IS NEW.result_schema_version
          AND intent.result_schema_fingerprint IS NEW.result_schema_fingerprint
          AND started.start_marker_id IS NEW.start_marker_id
          AND started.committed_at <= NEW.evaluated_at
          AND stage.status = 'running' AND stage.revision = 2
          AND lease.status = 'reserved'
          AND lease.holder_id IS NEW.lease_holder_id
          AND lease.fence_token IS NEW.fence_token
          AND terminal.sequence IS NEW.terminal_sequence
          AND terminal.stream_version IS NEW.terminal_stream_version
          AND terminal.stream_id IS NEW.thread_id
          AND terminal.event_type = 'thread.session-set'
          AND json_extract(
            terminal.metadata_json, '$.providerRuntimeLifecycle.runtimeEventId'
          ) IS delivery.terminal_event_id
          AND json_extract(
            terminal.metadata_json, '$.providerRuntimeLifecycle.providerState'
          ) = 'completed'
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.handoffId'
          ) IS NEW.handoff_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.providerDeliveryId'
          ) IS NEW.provider_delivery_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.providerInstanceId'
          ) IS NEW.provider_instance_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.providerTurnId'
          ) IS NEW.provider_turn_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.resultSchemaFingerprint'
          ) IS NEW.result_schema_fingerprint
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.sourceDisposition'
          ) IS NEW.source_disposition
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.finalMessageId'
          ) IS NEW.source_message_id
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.outputDigest'
          ) IS NEW.raw_output_digest
          AND json_extract(
            terminal.metadata_json, '$.verificationResultSource.outputByteLength'
          ) IS NEW.output_byte_length
          AND (
            (NEW.source_event_id IS NULL AND source.event_id IS NULL)
            OR (
              source.event_id IS NEW.source_event_id
              AND source.sequence IS NEW.source_event_sequence
              AND source.stream_version IS NEW.source_event_stream_version
              AND source.stream_id IS NEW.thread_id
              AND source.event_type IN (
                'thread.message-sent', 'thread.verification-result-fragment-captured'
              )
              AND json_extract(source.payload_json, '$.messageId') IS NEW.source_message_id
              AND json_extract(source.payload_json, '$.turnId') IS NEW.provider_turn_id
              AND (
                (source.event_type = 'thread.message-sent'
                  AND json_extract(source.payload_json, '$.role') = 'assistant'
                  AND json_extract(source.payload_json, '$.streaming') = 0)
                OR
                (source.event_type = 'thread.verification-result-fragment-captured'
                  AND json_extract(source.payload_json, '$.fragment.kind') = 'completion'
                  AND json_extract(
                    source.metadata_json, '$.verificationResultCapture.disposition'
                  ) = 'authority'
                  AND json_extract(
                    source.metadata_json, '$.verificationResultCapture.handoffId'
                  ) IS NEW.handoff_id
                  AND json_extract(
                    source.metadata_json, '$.verificationResultCapture.providerDeliveryId'
                  ) IS NEW.provider_delivery_id)
              )
              AND json_extract(
                source.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
              ) IS NEW.provider_instance_id
              AND json_extract(
                source.metadata_json, '$.providerRuntimeMessage.providerTurnId'
              ) IS NEW.provider_turn_id
            )
          )
      )
      BEGIN SELECT RAISE(ABORT, 'verification evaluation authority is inconsistent'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_evaluation_receipt_validate
      BEFORE INSERT ON agent_control_verification_evaluation_receipts
      WHEN NOT EXISTS (
        SELECT 1 FROM agent_control_verification_evaluation_evidence evidence
        WHERE evidence.evaluation_id IS NEW.evaluation_id
          AND evidence.evidence_id IS NEW.evidence_id
          AND evidence.marker_id IS NEW.marker_id
          AND evidence.evaluation_fingerprint IS NEW.evaluation_fingerprint
          AND evidence.provider_delivery_id IS NEW.provider_delivery_id
          AND evidence.provider_instance_id IS NEW.provider_instance_id
          AND evidence.provider_turn_id IS NEW.provider_turn_id
          AND evidence.terminal_event_id IS NEW.terminal_event_id
          AND evidence.terminal_observation_digest IS NEW.terminal_observation_digest
          AND evidence.source_message_id IS NEW.source_message_id
          AND evidence.source_event_id IS NEW.source_event_id
          AND evidence.raw_output_digest IS NEW.raw_output_digest
          AND evidence.output_byte_length IS NEW.output_byte_length
          AND evidence.source_disposition IS NEW.source_disposition
          AND evidence.disposition IS NEW.disposition
          AND evidence.verdict IS NEW.verdict
          AND evidence.error_code IS NEW.error_code
          AND evidence.evaluated_at IS NEW.accepted_at
      )
      BEGIN SELECT RAISE(ABORT, 'verification evaluation receipt is inconsistent'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER agent_control_verification_evaluation_marker_validate
      BEFORE INSERT ON agent_control_verification_evaluation_markers
      WHEN NOT EXISTS (
        SELECT 1
        FROM agent_control_verification_evaluation_evidence evidence
        JOIN agent_control_verification_evaluation_receipts receipt
          ON receipt.evaluation_id = evidence.evaluation_id
         AND receipt.evidence_id = evidence.evidence_id
        WHERE evidence.evaluation_id IS NEW.evaluation_id
          AND evidence.evidence_id IS NEW.evidence_id
          AND evidence.receipt_id IS NEW.receipt_id
          AND evidence.marker_id IS NEW.marker_id
          AND evidence.evaluation_fingerprint IS NEW.evaluation_fingerprint
          AND evidence.provider_delivery_id IS NEW.provider_delivery_id
          AND evidence.evaluated_at IS NEW.committed_at
          AND receipt.receipt_id IS NEW.receipt_id
          AND receipt.marker_id IS NEW.marker_id
          AND receipt.evaluation_fingerprint IS NEW.evaluation_fingerprint
          AND receipt.provider_delivery_id IS NEW.provider_delivery_id
          AND receipt.accepted_at IS NEW.committed_at
      )
      BEGIN SELECT RAISE(ABORT, 'verification evaluation marker is inconsistent'); END
    `).unprepared;
    for (const table of [
      "agent_control_verification_evaluation_evidence",
      "agent_control_verification_evaluation_receipts",
      "agent_control_verification_evaluation_markers",
    ]) {
      yield* sql.unsafe(`
        CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
        BEGIN SELECT RAISE(ABORT, 'verification evaluation evidence is immutable'); END
      `).unprepared;
      yield* sql.unsafe(`
        CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table}
        BEGIN SELECT RAISE(ABORT, 'verification evaluation evidence is immutable'); END
      `).unprepared;
    }
    yield* injectFault("after-install");

    const violations = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
    if (violations.length !== 0) {
      return yield* Effect.die(new Error("migration 060 introduced foreign-key violations"));
    }
    const integrity = yield* sql<{ readonly integrity_check: string }>`PRAGMA integrity_check`;
    if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") {
      return yield* Effect.die(new Error("migration 060 failed SQLite integrity validation"));
    }
  });

export default makeMigration060();
