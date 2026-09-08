import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { sha256Utf8 } from "../../agentControl/initialPlanning/eventEvidence.ts";

const CANONICAL_BLOB_MATCH = "t3_run_once_canonical_blob_match";

export const PROVIDER_ADMISSION_SCHEMA_OBJECTS = [
  "agent_control_provider_admission_intents",
  "agent_control_provider_usage_evidence",
  "agent_control_provider_claim_history",
  "agent_control_provider_authority_evidence",
  "agent_control_provider_authority_receipts",
  "agent_control_provider_authority_markers",
  "agent_control_provider_admission_current",
  "agent_control_provider_capacity_current",
  "idx_agent_control_provider_admission_queue",
  "idx_agent_control_provider_admission_deadline",
  "idx_agent_control_provider_admission_lease_deadline",
  "idx_agent_control_provider_authority_admission",
  "agent_control_provider_admission_intents_no_update",
  "agent_control_provider_admission_intents_no_delete",
  "agent_control_provider_usage_evidence_no_update",
  "agent_control_provider_usage_evidence_no_delete",
  "agent_control_provider_claim_history_no_update",
  "agent_control_provider_claim_history_no_delete",
  "agent_control_provider_authority_evidence_no_update",
  "agent_control_provider_authority_evidence_no_delete",
  "agent_control_provider_authority_receipts_no_update",
  "agent_control_provider_authority_receipts_no_delete",
  "agent_control_provider_authority_markers_no_update",
  "agent_control_provider_authority_markers_no_delete",
  "agent_control_provider_admission_intent_validate",
  "agent_control_provider_usage_evidence_validate",
  "agent_control_provider_authority_evidence_validate",
  "agent_control_provider_authority_receipt_validate",
  "agent_control_provider_authority_marker_validate",
  "agent_control_provider_admission_current_validate_insert",
  "agent_control_provider_admission_current_validate_update",
  "agent_control_provider_admission_current_no_delete",
  "agent_control_provider_capacity_current_validate_insert",
  "agent_control_provider_capacity_current_validate_update",
  "agent_control_provider_capacity_current_no_delete",
] as const;

const sha256Check = (column: string) =>
  `typeof(${column}) = 'text' AND length(${column}) = 64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;

const createTables = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TABLE main.agent_control_provider_admission_intents (
      admission_id TEXT PRIMARY KEY CHECK (typeof(admission_id)='text' AND length(admission_id)>64),
      stage TEXT NOT NULL CHECK (typeof(stage)='text' AND stage IN ('initial-planning','implementation','verification')),
      project_id TEXT NOT NULL CHECK (typeof(project_id)='text' AND length(project_id)>0),
      task_id TEXT NOT NULL CHECK (typeof(task_id)='text' AND length(task_id)>0),
      stage_run_id TEXT NOT NULL CHECK (typeof(stage_run_id)='text' AND length(stage_run_id)>0),
      attempt_id TEXT NOT NULL CHECK (typeof(attempt_id)='text' AND length(attempt_id)>0),
      handoff_id TEXT NOT NULL UNIQUE CHECK (typeof(handoff_id)='text' AND length(handoff_id)>0),
      provider_delivery_id TEXT NOT NULL UNIQUE CHECK (typeof(provider_delivery_id)='text' AND length(provider_delivery_id)>0),
      thread_id TEXT NOT NULL CHECK (typeof(thread_id)='text' AND length(thread_id)>0),
      provider_instance_id TEXT NOT NULL CHECK (typeof(provider_instance_id)='text' AND length(provider_instance_id)>0),
      stage_lease_id TEXT NOT NULL CHECK (typeof(stage_lease_id)='text' AND length(stage_lease_id)>0),
      stage_lease_holder_id TEXT NOT NULL CHECK (typeof(stage_lease_holder_id)='text' AND length(stage_lease_holder_id)>0),
      stage_fence_token INTEGER NOT NULL CHECK (typeof(stage_fence_token)='integer' AND stage_fence_token>=1),
      model_selection_json BLOB NOT NULL CHECK (typeof(model_selection_json)='blob'),
      model_selection_fingerprint TEXT NOT NULL CHECK (${sha256Check("model_selection_fingerprint")}),
      requested_at TEXT NOT NULL CHECK (typeof(requested_at)='text' AND requested_at=strftime('%Y-%m-%dT%H:%M:%fZ', requested_at)),
      intent_json BLOB NOT NULL CHECK (typeof(intent_json)='blob'),
      intent_fingerprint TEXT NOT NULL UNIQUE CHECK (${sha256Check("intent_fingerprint")})
    ) STRICT
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TABLE main.agent_control_provider_usage_evidence (
      evidence_id TEXT PRIMARY KEY CHECK (typeof(evidence_id)='text' AND length(evidence_id)>64),
      admission_id TEXT NOT NULL,
      provider_instance_id TEXT NOT NULL CHECK (typeof(provider_instance_id)='text' AND length(provider_instance_id)>0),
      status TEXT NOT NULL CHECK (typeof(status)='text' AND status IN ('allowed','warning','rejected','unsupported','supported-unusable')),
      source TEXT NOT NULL CHECK (typeof(source)='text' AND source IN ('refresh','runtime-event','capability','refresh-error')),
      observed_at TEXT NOT NULL CHECK (typeof(observed_at)='text' AND observed_at=strftime('%Y-%m-%dT%H:%M:%fZ', observed_at)),
      next_relevant_at TEXT CHECK (next_relevant_at IS NULL OR (typeof(next_relevant_at)='text' AND next_relevant_at=strftime('%Y-%m-%dT%H:%M:%fZ', next_relevant_at))),
      evidence_json BLOB NOT NULL CHECK (typeof(evidence_json)='blob'),
      evidence_fingerprint TEXT NOT NULL CHECK (${sha256Check("evidence_fingerprint")}),
      UNIQUE(admission_id, evidence_fingerprint),
      FOREIGN KEY(admission_id) REFERENCES agent_control_provider_admission_intents(admission_id)
    ) STRICT
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TABLE main.agent_control_provider_claim_history (
      claim_id TEXT PRIMARY KEY CHECK (typeof(claim_id)='text' AND length(claim_id)>64),
      admission_id TEXT NOT NULL,
      provider_instance_id TEXT NOT NULL CHECK (typeof(provider_instance_id)='text' AND length(provider_instance_id)>0),
      owner_id TEXT NOT NULL CHECK (typeof(owner_id)='text' AND length(owner_id)>0),
      provider_fence_token INTEGER NOT NULL CHECK (typeof(provider_fence_token)='integer' AND provider_fence_token>=1),
      claimed_at TEXT NOT NULL CHECK (typeof(claimed_at)='text' AND claimed_at=strftime('%Y-%m-%dT%H:%M:%fZ', claimed_at)),
      lease_expires_at TEXT NOT NULL CHECK (typeof(lease_expires_at)='text' AND lease_expires_at=strftime('%Y-%m-%dT%H:%M:%fZ', lease_expires_at) AND lease_expires_at>claimed_at),
      claim_fingerprint TEXT NOT NULL UNIQUE CHECK (${sha256Check("claim_fingerprint")}),
      UNIQUE(provider_instance_id, provider_fence_token),
      UNIQUE(admission_id, provider_fence_token),
      FOREIGN KEY(admission_id) REFERENCES agent_control_provider_admission_intents(admission_id)
    ) STRICT
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TABLE main.agent_control_provider_authority_evidence (
      evidence_id TEXT PRIMARY KEY CHECK (typeof(evidence_id)='text' AND length(evidence_id)>64),
      receipt_id TEXT NOT NULL UNIQUE CHECK (typeof(receipt_id)='text' AND length(receipt_id)>64),
      marker_id TEXT NOT NULL UNIQUE CHECK (typeof(marker_id)='text' AND length(marker_id)>64),
      admission_id TEXT NOT NULL,
      authority_kind TEXT NOT NULL CHECK (typeof(authority_kind)='text' AND authority_kind IN ('admission','session-entry','turn-entry','quarantine','supersede','release')),
      provider_instance_id TEXT NOT NULL CHECK (typeof(provider_instance_id)='text' AND length(provider_instance_id)>0),
      owner_id TEXT NOT NULL CHECK (typeof(owner_id)='text' AND length(owner_id)>0),
      provider_fence_token INTEGER NOT NULL CHECK (typeof(provider_fence_token)='integer' AND (provider_fence_token>=1 OR (authority_kind='supersede' AND provider_fence_token=0))),
      occurred_at TEXT NOT NULL CHECK (typeof(occurred_at)='text' AND occurred_at=strftime('%Y-%m-%dT%H:%M:%fZ', occurred_at)),
      terminal_runtime_event_id TEXT,
      terminal_event_type TEXT,
      terminal_stream_version INTEGER,
      finalization_marker_id TEXT,
      finalization_marker_fingerprint TEXT,
      payload_json BLOB NOT NULL CHECK (typeof(payload_json)='blob'),
      payload_fingerprint TEXT NOT NULL UNIQUE CHECK (${sha256Check("payload_fingerprint")}),
      CHECK ((authority_kind='release' AND typeof(terminal_runtime_event_id)='text' AND length(terminal_runtime_event_id)>0 AND typeof(terminal_event_type)='text' AND length(terminal_event_type)>0 AND typeof(terminal_stream_version)='integer' AND terminal_stream_version>=1 AND typeof(finalization_marker_id)='text' AND length(finalization_marker_id)>0 AND ${sha256Check("finalization_marker_fingerprint")}) OR (authority_kind='supersede' AND (terminal_runtime_event_id IS NULL OR (typeof(terminal_runtime_event_id)='text' AND length(terminal_runtime_event_id)>0)) AND typeof(terminal_event_type)='text' AND length(terminal_event_type)>0 AND typeof(terminal_stream_version)='integer' AND terminal_stream_version>=1 AND typeof(finalization_marker_id)='text' AND length(finalization_marker_id)>0 AND ${sha256Check("finalization_marker_fingerprint")}) OR (authority_kind NOT IN ('release','supersede') AND terminal_runtime_event_id IS NULL AND terminal_event_type IS NULL AND terminal_stream_version IS NULL AND finalization_marker_id IS NULL AND finalization_marker_fingerprint IS NULL)),
      UNIQUE(admission_id, authority_kind, provider_fence_token),
      FOREIGN KEY(admission_id) REFERENCES agent_control_provider_admission_intents(admission_id),
      FOREIGN KEY(receipt_id) REFERENCES agent_control_provider_authority_receipts(receipt_id) DEFERRABLE INITIALLY DEFERRED,
      FOREIGN KEY(marker_id) REFERENCES agent_control_provider_authority_markers(marker_id) DEFERRABLE INITIALLY DEFERRED
    ) STRICT
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TABLE main.agent_control_provider_authority_receipts (
      receipt_id TEXT PRIMARY KEY CHECK (typeof(receipt_id)='text' AND length(receipt_id)>64),
      evidence_id TEXT NOT NULL UNIQUE,
      marker_id TEXT NOT NULL UNIQUE,
      admission_id TEXT NOT NULL,
      authority_kind TEXT NOT NULL CHECK (typeof(authority_kind)='text' AND authority_kind IN ('admission','session-entry','turn-entry','quarantine','supersede','release')),
      status TEXT NOT NULL CHECK (typeof(status)='text' AND status='accepted'),
      accepted_at TEXT NOT NULL CHECK (typeof(accepted_at)='text' AND accepted_at=strftime('%Y-%m-%dT%H:%M:%fZ', accepted_at)),
      FOREIGN KEY(evidence_id) REFERENCES agent_control_provider_authority_evidence(evidence_id),
      FOREIGN KEY(marker_id) REFERENCES agent_control_provider_authority_markers(marker_id) DEFERRABLE INITIALLY DEFERRED
    ) STRICT
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TABLE main.agent_control_provider_authority_markers (
      marker_id TEXT PRIMARY KEY CHECK (typeof(marker_id)='text' AND length(marker_id)>64),
      evidence_id TEXT NOT NULL UNIQUE,
      receipt_id TEXT NOT NULL UNIQUE,
      admission_id TEXT NOT NULL,
      authority_kind TEXT NOT NULL CHECK (typeof(authority_kind)='text' AND authority_kind IN ('admission','session-entry','turn-entry','quarantine','supersede','release')),
      marker_fingerprint TEXT NOT NULL UNIQUE CHECK (${sha256Check("marker_fingerprint")}),
      committed_at TEXT NOT NULL CHECK (typeof(committed_at)='text' AND committed_at=strftime('%Y-%m-%dT%H:%M:%fZ', committed_at)),
      FOREIGN KEY(evidence_id) REFERENCES agent_control_provider_authority_evidence(evidence_id),
      FOREIGN KEY(receipt_id) REFERENCES agent_control_provider_authority_receipts(receipt_id)
    ) STRICT
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TABLE main.agent_control_provider_admission_current (
      admission_id TEXT PRIMARY KEY,
      provider_instance_id TEXT NOT NULL CHECK (typeof(provider_instance_id)='text' AND length(provider_instance_id)>0),
      stage TEXT NOT NULL CHECK (typeof(stage)='text' AND stage IN ('initial-planning','implementation','verification')),
      handoff_id TEXT NOT NULL UNIQUE CHECK (typeof(handoff_id)='text' AND length(handoff_id)>0),
      status TEXT NOT NULL CHECK (typeof(status)='text' AND status IN ('waiting','claimed','admitted','entered','quarantined','released','superseded')),
      requested_at TEXT NOT NULL CHECK (typeof(requested_at)='text' AND requested_at=strftime('%Y-%m-%dT%H:%M:%fZ', requested_at)),
      usage_status TEXT NOT NULL CHECK (typeof(usage_status)='text' AND usage_status IN ('allowed','warning','rejected','unsupported','supported-unusable')),
      usage_eligible INTEGER NOT NULL CHECK (typeof(usage_eligible)='integer' AND usage_eligible IN (0,1)),
      usage_evidence_id TEXT NOT NULL,
      usage_evidence_fingerprint TEXT NOT NULL CHECK (${sha256Check("usage_evidence_fingerprint")}),
      next_deadline_at TEXT CHECK (next_deadline_at IS NULL OR (typeof(next_deadline_at)='text' AND next_deadline_at=strftime('%Y-%m-%dT%H:%M:%fZ', next_deadline_at))),
      owner_id TEXT,
      lease_expires_at TEXT,
      provider_fence_token INTEGER,
      admission_marker_id TEXT,
      admission_marker_fingerprint TEXT,
      revision INTEGER NOT NULL CHECK (typeof(revision)='integer' AND revision>=1),
      updated_at TEXT NOT NULL CHECK (typeof(updated_at)='text' AND updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', updated_at)),
      CHECK (usage_eligible=CASE WHEN usage_status IN ('allowed','warning','unsupported') THEN 1 ELSE 0 END),
      CHECK (
        (status='waiting' AND owner_id IS NULL AND lease_expires_at IS NULL AND provider_fence_token IS NULL AND admission_marker_id IS NULL AND admission_marker_fingerprint IS NULL)
        OR (status='superseded' AND owner_id IS NULL AND lease_expires_at IS NULL AND provider_fence_token IS NULL AND admission_marker_id IS NULL AND admission_marker_fingerprint IS NULL)
        OR (status IN ('claimed','admitted','entered','quarantined','released','superseded') AND typeof(owner_id)='text' AND length(owner_id)>0 AND typeof(lease_expires_at)='text' AND typeof(provider_fence_token)='integer' AND provider_fence_token>=1 AND ((status='claimed' AND admission_marker_id IS NULL AND admission_marker_fingerprint IS NULL) OR (status!='claimed' AND typeof(admission_marker_id)='text' AND length(admission_marker_id)>64 AND ${sha256Check("admission_marker_fingerprint")})))
      ),
      FOREIGN KEY(admission_id) REFERENCES agent_control_provider_admission_intents(admission_id),
      FOREIGN KEY(usage_evidence_id) REFERENCES agent_control_provider_usage_evidence(evidence_id)
    ) STRICT
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TABLE main.agent_control_provider_capacity_current (
      provider_instance_id TEXT PRIMARY KEY CHECK (typeof(provider_instance_id)='text' AND length(provider_instance_id)>0),
      last_fence_token INTEGER NOT NULL CHECK (typeof(last_fence_token)='integer' AND last_fence_token>=0),
      active_admission_id TEXT UNIQUE,
      active_state TEXT CHECK (active_state IS NULL OR (typeof(active_state)='text' AND active_state IN ('claimed','admitted','entered','quarantined'))),
      active_owner_id TEXT,
      active_lease_expires_at TEXT,
      active_fence_token INTEGER,
      active_marker_fingerprint TEXT,
      revision INTEGER NOT NULL CHECK (typeof(revision)='integer' AND revision>=1),
      updated_at TEXT NOT NULL CHECK (typeof(updated_at)='text' AND updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', updated_at)),
      CHECK ((active_admission_id IS NULL AND active_state IS NULL AND active_owner_id IS NULL AND active_lease_expires_at IS NULL AND active_fence_token IS NULL AND active_marker_fingerprint IS NULL) OR (typeof(active_admission_id)='text' AND length(active_admission_id)>64 AND typeof(active_state)='text' AND typeof(active_owner_id)='text' AND length(active_owner_id)>0 AND typeof(active_lease_expires_at)='text' AND typeof(active_fence_token)='integer' AND active_fence_token=last_fence_token AND (active_marker_fingerprint IS NULL OR (${sha256Check("active_marker_fingerprint")})))),
      FOREIGN KEY(active_admission_id) REFERENCES agent_control_provider_admission_intents(admission_id)
    ) STRICT
  `).unprepared;
});

const createIndexes = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE INDEX main.idx_agent_control_provider_admission_queue ON agent_control_provider_admission_current(provider_instance_id,status,usage_eligible,requested_at,admission_id)`;
  yield* sql`CREATE INDEX main.idx_agent_control_provider_admission_deadline ON agent_control_provider_admission_current(status,next_deadline_at,provider_instance_id,admission_id)`;
  yield* sql`CREATE INDEX main.idx_agent_control_provider_admission_lease_deadline ON agent_control_provider_admission_current(status,lease_expires_at,provider_instance_id,admission_id)`;
  yield* sql`CREATE INDEX main.idx_agent_control_provider_authority_admission ON agent_control_provider_authority_markers(admission_id,authority_kind,committed_at)`;
});

const createAppendOnlyTriggers = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const table of [
    "admission_intents",
    "usage_evidence",
    "claim_history",
    "authority_evidence",
    "authority_receipts",
    "authority_markers",
  ] as const) {
    yield* sql.unsafe(
      `CREATE TRIGGER main.agent_control_provider_${table}_no_update BEFORE UPDATE ON agent_control_provider_${table} BEGIN SELECT RAISE(ABORT, 'provider admission history is append-only'); END`,
    ).unprepared;
    yield* sql.unsafe(
      `CREATE TRIGGER main.agent_control_provider_${table}_no_delete BEFORE DELETE ON agent_control_provider_${table} BEGIN SELECT RAISE(ABORT, 'provider admission history is append-only'); END`,
    ).unprepared;
  }
  for (const table of ["admission_current", "capacity_current"] as const) {
    yield* sql.unsafe(
      `CREATE TRIGGER main.agent_control_provider_${table}_no_delete BEFORE DELETE ON agent_control_provider_${table} BEGIN SELECT RAISE(ABORT, 'provider admission projection cannot be deleted'); END`,
    ).unprepared;
  }
});

const createValidationTriggers = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_provider_admission_intent_validate
    BEFORE INSERT ON agent_control_provider_admission_intents
    WHEN NOT (
      ${CANONICAL_BLOB_MATCH}(NEW.model_selection_json, NEW.model_selection_fingerprint)=1
      AND ${CANONICAL_BLOB_MATCH}(NEW.intent_json, NEW.intent_fingerprint)=1
      AND json_extract(CAST(NEW.intent_json AS TEXT),'$.admissionId')=NEW.admission_id
      AND json_extract(CAST(NEW.intent_json AS TEXT),'$.stage')=NEW.stage
      AND json_extract(CAST(NEW.intent_json AS TEXT),'$.providerInstanceId')=NEW.provider_instance_id
      AND json_extract(CAST(NEW.intent_json AS TEXT),'$.providerDeliveryId')=NEW.provider_delivery_id
      AND json_extract(CAST(NEW.intent_json AS TEXT),'$.handoffId')=NEW.handoff_id
      AND json_extract(CAST(NEW.intent_json AS TEXT),'$.modelSelectionFingerprint')=NEW.model_selection_fingerprint
    ) BEGIN SELECT RAISE(ABORT, 'provider admission intent is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_provider_usage_evidence_validate
    BEFORE INSERT ON agent_control_provider_usage_evidence
    WHEN NOT (
      ${CANONICAL_BLOB_MATCH}(NEW.evidence_json, NEW.evidence_fingerprint)=1
      AND json_extract(CAST(NEW.evidence_json AS TEXT),'$.admissionId')=NEW.admission_id
      AND json_extract(CAST(NEW.evidence_json AS TEXT),'$.providerInstanceId')=NEW.provider_instance_id
      AND json_extract(CAST(NEW.evidence_json AS TEXT),'$.status')=NEW.status
      AND json_extract(CAST(NEW.evidence_json AS TEXT),'$.source')=NEW.source
      AND json_extract(CAST(NEW.evidence_json AS TEXT),'$.observedAt')=NEW.observed_at
      AND json_extract(CAST(NEW.evidence_json AS TEXT),'$.nextRelevantAt') IS NEW.next_relevant_at
      AND EXISTS (SELECT 1 FROM main.agent_control_provider_admission_intents intent WHERE intent.admission_id=NEW.admission_id AND intent.provider_instance_id=NEW.provider_instance_id)
    ) BEGIN SELECT RAISE(ABORT, 'provider usage evidence is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_provider_authority_evidence_validate
    BEFORE INSERT ON agent_control_provider_authority_evidence
    WHEN NOT (
      ${CANONICAL_BLOB_MATCH}(NEW.payload_json, NEW.payload_fingerprint)=1
      AND json_extract(CAST(NEW.payload_json AS TEXT),'$.admissionId')=NEW.admission_id
      AND json_extract(CAST(NEW.payload_json AS TEXT),'$.authorityKind')=NEW.authority_kind
      AND json_extract(CAST(NEW.payload_json AS TEXT),'$.providerInstanceId')=NEW.provider_instance_id
      AND json_extract(CAST(NEW.payload_json AS TEXT),'$.ownerId')=NEW.owner_id
      AND json_extract(CAST(NEW.payload_json AS TEXT),'$.providerFenceToken')=NEW.provider_fence_token
      AND json_extract(CAST(NEW.payload_json AS TEXT),'$.occurredAt')=NEW.occurred_at
      AND ((NEW.authority_kind='release'
        AND json_extract(CAST(NEW.payload_json AS TEXT),'$.terminalRuntimeEventId')=NEW.terminal_runtime_event_id
        AND json_extract(CAST(NEW.payload_json AS TEXT),'$.terminalEventType')=NEW.terminal_event_type
        AND json_extract(CAST(NEW.payload_json AS TEXT),'$.terminalStreamVersion')=NEW.terminal_stream_version
        AND json_extract(CAST(NEW.payload_json AS TEXT),'$.finalizationMarkerId')=NEW.finalization_marker_id
        AND json_extract(CAST(NEW.payload_json AS TEXT),'$.finalizationMarkerFingerprint')=NEW.finalization_marker_fingerprint)
        OR (NEW.authority_kind='supersede'
          AND json_extract(CAST(NEW.payload_json AS TEXT),'$.terminalRuntimeEventId') IS NEW.terminal_runtime_event_id
          AND json_extract(CAST(NEW.payload_json AS TEXT),'$.terminalEventType')=NEW.terminal_event_type
          AND json_extract(CAST(NEW.payload_json AS TEXT),'$.terminalStreamVersion')=NEW.terminal_stream_version
          AND json_extract(CAST(NEW.payload_json AS TEXT),'$.finalizationMarkerId')=NEW.finalization_marker_id
          AND json_extract(CAST(NEW.payload_json AS TEXT),'$.finalizationMarkerFingerprint')=NEW.finalization_marker_fingerprint)
        OR NEW.authority_kind NOT IN ('release','supersede'))
    ) BEGIN SELECT RAISE(ABORT, 'provider authority evidence is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_provider_authority_receipt_validate
    BEFORE INSERT ON agent_control_provider_authority_receipts
    WHEN NOT EXISTS (
      SELECT 1 FROM main.agent_control_provider_authority_evidence evidence
      WHERE evidence.evidence_id=NEW.evidence_id AND evidence.receipt_id=NEW.receipt_id
        AND evidence.marker_id=NEW.marker_id AND evidence.admission_id=NEW.admission_id
        AND evidence.authority_kind=NEW.authority_kind AND evidence.occurred_at=NEW.accepted_at
        AND NEW.status='accepted'
    ) BEGIN SELECT RAISE(ABORT, 'provider authority receipt is inconsistent'); END
  `).unprepared;
  yield* sql.unsafe(`
    CREATE TRIGGER main.agent_control_provider_authority_marker_validate
    BEFORE INSERT ON agent_control_provider_authority_markers
    WHEN NOT EXISTS (
      SELECT 1 FROM main.agent_control_provider_authority_evidence evidence
      JOIN main.agent_control_provider_authority_receipts receipt ON receipt.receipt_id=evidence.receipt_id
      WHERE evidence.evidence_id=NEW.evidence_id AND evidence.receipt_id=NEW.receipt_id
        AND evidence.marker_id=NEW.marker_id AND evidence.admission_id=NEW.admission_id
        AND evidence.authority_kind=NEW.authority_kind AND receipt.evidence_id=NEW.evidence_id
        AND receipt.marker_id=NEW.marker_id AND receipt.admission_id=NEW.admission_id
        AND receipt.authority_kind=NEW.authority_kind AND receipt.accepted_at=NEW.committed_at
        AND evidence.occurred_at=NEW.committed_at AND evidence.payload_fingerprint=NEW.marker_fingerprint
    ) BEGIN SELECT RAISE(ABORT, 'provider authority marker is inconsistent'); END
  `).unprepared;
  for (const operation of ["INSERT", "UPDATE"] as const) {
    const suffix = operation === "INSERT" ? "insert" : "update";
    const admissionTransition =
      operation === "INSERT"
        ? `NEW.revision=1 AND NEW.status='waiting'`
        : `NEW.revision=OLD.revision+1
          AND NEW.admission_id=OLD.admission_id
          AND NEW.provider_instance_id=OLD.provider_instance_id
          AND NEW.stage=OLD.stage
          AND NEW.handoff_id=OLD.handoff_id
          AND NEW.requested_at=OLD.requested_at
          AND NEW.updated_at>=OLD.updated_at
          AND (
            (OLD.status='waiting' AND NEW.status='waiting')
            OR (OLD.status='waiting' AND NEW.status='claimed'
              AND NEW.usage_status=OLD.usage_status
              AND NEW.usage_evidence_id=OLD.usage_evidence_id
              AND NEW.usage_evidence_fingerprint=OLD.usage_evidence_fingerprint
              AND NEW.next_deadline_at IS OLD.next_deadline_at)
            OR (OLD.status='waiting' AND NEW.status='superseded'
              AND NEW.usage_status=OLD.usage_status
              AND NEW.usage_evidence_id=OLD.usage_evidence_id
              AND NEW.usage_evidence_fingerprint=OLD.usage_evidence_fingerprint
              AND NEW.next_deadline_at IS OLD.next_deadline_at)
            OR (OLD.status='claimed' AND NEW.status='admitted'
              AND NEW.owner_id=OLD.owner_id
              AND NEW.lease_expires_at=OLD.lease_expires_at
              AND NEW.provider_fence_token=OLD.provider_fence_token)
            OR (OLD.status='admitted' AND NEW.status='admitted'
              AND NEW.provider_fence_token=OLD.provider_fence_token+1)
            OR (OLD.status='admitted' AND NEW.status IN ('entered','superseded')
              AND NEW.owner_id=OLD.owner_id
              AND NEW.lease_expires_at=OLD.lease_expires_at
              AND NEW.provider_fence_token=OLD.provider_fence_token
              AND NEW.admission_marker_id=OLD.admission_marker_id
              AND NEW.admission_marker_fingerprint=OLD.admission_marker_fingerprint)
            OR (OLD.status='entered' AND NEW.status IN ('quarantined','released')
              AND NEW.owner_id=OLD.owner_id
              AND NEW.lease_expires_at=OLD.lease_expires_at
              AND NEW.provider_fence_token=OLD.provider_fence_token
              AND NEW.admission_marker_id=OLD.admission_marker_id
              AND NEW.admission_marker_fingerprint=OLD.admission_marker_fingerprint)
            OR (OLD.status='quarantined' AND NEW.status='released'
              AND NEW.owner_id=OLD.owner_id
              AND NEW.lease_expires_at=OLD.lease_expires_at
              AND NEW.provider_fence_token=OLD.provider_fence_token
              AND NEW.admission_marker_id=OLD.admission_marker_id
              AND NEW.admission_marker_fingerprint=OLD.admission_marker_fingerprint)
          )
          AND (OLD.status='waiting' AND NEW.status='waiting' OR (
            NEW.usage_status=OLD.usage_status
            AND NEW.usage_eligible=OLD.usage_eligible
            AND NEW.usage_evidence_id=OLD.usage_evidence_id
            AND NEW.usage_evidence_fingerprint=OLD.usage_evidence_fingerprint
            AND NEW.next_deadline_at IS OLD.next_deadline_at
          ))`;
    const capacityTransition =
      operation === "INSERT"
        ? `NEW.revision=1 AND NEW.last_fence_token=0 AND NEW.active_admission_id IS NULL`
        : `NEW.revision=OLD.revision+1
          AND NEW.provider_instance_id=OLD.provider_instance_id
          AND NEW.updated_at>=OLD.updated_at
          AND (
            (OLD.active_state IS NULL AND NEW.active_state='claimed'
              AND NEW.last_fence_token=OLD.last_fence_token+1)
            OR (OLD.active_state='claimed' AND NEW.active_state='admitted'
              AND NEW.last_fence_token=OLD.last_fence_token
              AND NEW.active_admission_id=OLD.active_admission_id
              AND NEW.active_owner_id=OLD.active_owner_id
              AND NEW.active_lease_expires_at=OLD.active_lease_expires_at
              AND NEW.active_fence_token=OLD.active_fence_token)
            OR (OLD.active_state='admitted' AND NEW.active_state='admitted'
              AND NEW.last_fence_token=OLD.last_fence_token+1
              AND NEW.active_admission_id=OLD.active_admission_id)
            OR (OLD.active_state='admitted' AND NEW.active_state='entered'
              AND NEW.last_fence_token=OLD.last_fence_token
              AND NEW.active_admission_id=OLD.active_admission_id
              AND NEW.active_owner_id=OLD.active_owner_id
              AND NEW.active_lease_expires_at=OLD.active_lease_expires_at
              AND NEW.active_fence_token=OLD.active_fence_token
              AND NEW.active_marker_fingerprint=OLD.active_marker_fingerprint)
            OR (OLD.active_state='entered' AND NEW.active_state='quarantined'
              AND NEW.last_fence_token=OLD.last_fence_token
              AND NEW.active_admission_id=OLD.active_admission_id
              AND NEW.active_owner_id=OLD.active_owner_id
              AND NEW.active_lease_expires_at=OLD.active_lease_expires_at
              AND NEW.active_fence_token=OLD.active_fence_token
              AND NEW.active_marker_fingerprint=OLD.active_marker_fingerprint)
            OR (OLD.active_state IN ('admitted','entered','quarantined')
              AND NEW.active_state IS NULL
              AND NEW.last_fence_token=OLD.last_fence_token
              AND EXISTS (
                SELECT 1 FROM main.agent_control_provider_admission_current admission
                WHERE admission.admission_id=OLD.active_admission_id
                  AND admission.provider_instance_id=OLD.provider_instance_id
                  AND admission.status IN ('released','superseded')
              ))
          )`;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_provider_admission_current_validate_${suffix}
      BEFORE ${operation} ON agent_control_provider_admission_current
      WHEN NOT (
        ${admissionTransition}
        AND EXISTS (SELECT 1 FROM main.agent_control_provider_admission_intents intent WHERE intent.admission_id=NEW.admission_id AND intent.provider_instance_id=NEW.provider_instance_id AND intent.stage=NEW.stage AND intent.handoff_id=NEW.handoff_id AND intent.requested_at=NEW.requested_at)
        AND EXISTS (SELECT 1 FROM main.agent_control_provider_usage_evidence usage WHERE usage.evidence_id=NEW.usage_evidence_id AND usage.admission_id=NEW.admission_id AND usage.provider_instance_id=NEW.provider_instance_id AND usage.status=NEW.usage_status AND usage.evidence_fingerprint=NEW.usage_evidence_fingerprint AND usage.next_relevant_at IS NEW.next_deadline_at)
        AND (NEW.admission_marker_id IS NULL OR EXISTS (
          SELECT 1 FROM main.agent_control_provider_authority_markers marker
          JOIN main.agent_control_provider_authority_evidence evidence
            ON evidence.evidence_id=marker.evidence_id
          WHERE marker.marker_id=NEW.admission_marker_id
            AND marker.admission_id=NEW.admission_id
            AND marker.authority_kind='admission'
            AND marker.marker_fingerprint=NEW.admission_marker_fingerprint
            AND evidence.owner_id=NEW.owner_id
            AND evidence.provider_fence_token=NEW.provider_fence_token
            AND NOT EXISTS (
              SELECT 1 FROM main.agent_control_provider_authority_evidence newer
              WHERE newer.admission_id=NEW.admission_id
                AND newer.authority_kind='admission'
                AND newer.provider_fence_token>evidence.provider_fence_token
            )
        ))
        AND CASE NEW.status
          WHEN 'waiting' THEN NEW.admission_marker_id IS NULL
          WHEN 'claimed' THEN NEW.admission_marker_id IS NULL
          WHEN 'admitted' THEN NEW.admission_marker_id IS NOT NULL
          WHEN 'entered' THEN EXISTS (
            SELECT 1 FROM main.agent_control_provider_authority_markers marker
            JOIN main.agent_control_provider_authority_evidence evidence
              ON evidence.evidence_id=marker.evidence_id
            WHERE marker.admission_id=NEW.admission_id
              AND marker.authority_kind IN ('session-entry','turn-entry')
              AND evidence.owner_id=NEW.owner_id
              AND evidence.provider_fence_token=NEW.provider_fence_token
          )
          WHEN 'quarantined' THEN EXISTS (
            SELECT 1 FROM main.agent_control_provider_authority_markers marker
            JOIN main.agent_control_provider_authority_evidence evidence
              ON evidence.evidence_id=marker.evidence_id
            WHERE marker.admission_id=NEW.admission_id
              AND marker.authority_kind='quarantine'
              AND evidence.owner_id=NEW.owner_id
              AND evidence.provider_fence_token=NEW.provider_fence_token
          )
          WHEN 'released' THEN EXISTS (
            SELECT 1 FROM main.agent_control_provider_authority_markers marker
            JOIN main.agent_control_provider_authority_evidence evidence
              ON evidence.evidence_id=marker.evidence_id
            WHERE marker.admission_id=NEW.admission_id
              AND marker.authority_kind='release'
              AND evidence.owner_id=NEW.owner_id
              AND evidence.provider_fence_token=NEW.provider_fence_token
          )
          WHEN 'superseded' THEN EXISTS (
            SELECT 1 FROM main.agent_control_provider_authority_markers marker
            JOIN main.agent_control_provider_authority_evidence evidence
              ON evidence.evidence_id=marker.evidence_id
            WHERE marker.admission_id=NEW.admission_id
              AND marker.authority_kind='supersede'
              AND ((NEW.provider_fence_token IS NULL AND evidence.provider_fence_token=0)
                OR (evidence.owner_id=NEW.owner_id
                  AND evidence.provider_fence_token=NEW.provider_fence_token))
          )
          ELSE 0
        END
      ) BEGIN SELECT RAISE(ABORT, 'provider admission projection is inconsistent'); END
    `).unprepared;
    yield* sql.unsafe(`
      CREATE TRIGGER main.agent_control_provider_capacity_current_validate_${suffix}
      BEFORE ${operation} ON agent_control_provider_capacity_current
      WHEN NOT (
        ${capacityTransition}
        AND (NEW.active_admission_id IS NULL OR EXISTS (
          SELECT 1 FROM main.agent_control_provider_admission_current admission
          WHERE admission.admission_id=NEW.active_admission_id
            AND admission.provider_instance_id=NEW.provider_instance_id
            AND admission.status=NEW.active_state
            AND admission.owner_id=NEW.active_owner_id
            AND admission.lease_expires_at=NEW.active_lease_expires_at
            AND admission.provider_fence_token=NEW.active_fence_token
            AND admission.admission_marker_fingerprint IS NEW.active_marker_fingerprint
        ))
      ) BEGIN SELECT RAISE(ABORT, 'provider capacity projection is inconsistent'); END
    `).unprepared;
  }
});

export type Migration065FaultPoint = "after-tables" | "after-indexes" | "after-triggers";

// Filled with literal hashes after the DDL is frozen. A computed-at-runtime
// fingerprint would not detect source/schema drift.
export const EXPECTED_PROVIDER_ADMISSION_DDL_FINGERPRINTS: Readonly<Record<string, string>> = {
  agent_control_provider_admission_current:
    "00e59faa5f0d4d16bd96b6bacd4c121545bfdab095d166a42174e8cfb10cdd96",
  agent_control_provider_admission_current_validate_insert:
    "1767c5f6563e7cfe00443cda7a53d9316f67c02c0436a2a2fd140d76a22bcce2",
  agent_control_provider_admission_current_validate_update:
    "262a2e66c9eaa379d535ba94bede9f4cb294531a2e53713acf5f62c0a90fd9f6",
  agent_control_provider_admission_current_no_delete:
    "e0162fe23c71227178e41f49162672af7fd37d2e698b18a35898c5c8e2a73220",
  agent_control_provider_admission_intent_validate:
    "dec0f149bd4ed3454c783f3d644f87bc7015f7dd0935a7d640694621a86fe897",
  agent_control_provider_admission_intents:
    "f02723aacee65b629b5083cae24dea7fc585720df98d9bfb552dd785ec54bddc",
  agent_control_provider_admission_intents_no_delete:
    "d396b6e119337d5011d2de2e5183041c30ec87471b05c0e30de8cb2221933570",
  agent_control_provider_admission_intents_no_update:
    "c44a1d34a0e5dd8fa192e72d2b74a15525b9dc60ccd751d5d866d68a0ab13da1",
  agent_control_provider_authority_evidence:
    "94b9d513ca8efa92c4c85b9396bea40ef12214033a556b5eb20ed6f4ef7bf9c3",
  agent_control_provider_authority_evidence_validate:
    "92d0cb899180b3ef6d57da6a8e0cca736a1c9d6ac5ea5e35a4d86e1a65135710",
  agent_control_provider_authority_evidence_no_delete:
    "bd15ac76694d03c42c34ccf2e3b4110dbae2fa912dab8f642fd4ea22777206ce",
  agent_control_provider_authority_evidence_no_update:
    "0d4dc0b9b4dabecd92127d3fe6c4df95966fb5fac95ed10017507cfeb6d5f40f",
  agent_control_provider_authority_marker_validate:
    "8764264aabcc3de2797e56c04deb49dea5a399d2315006d0564228ab1474ffa8",
  agent_control_provider_authority_markers:
    "96ffe10d9f55a251b7914a869764b0380b64ae3813f0057c6b7035b821498162",
  agent_control_provider_authority_markers_no_delete:
    "cf2f275f0fbc8a8ec954fa8bdc4e85f8382b39c466c7564c8db123e076be9123",
  agent_control_provider_authority_markers_no_update:
    "b4864629ad61e43695a24ba5503db5fd6493a5d795b6b1537f772124da00f5f7",
  agent_control_provider_authority_receipt_validate:
    "b6d56bb11ec2514cc4806e2fe4e51014677fa1fb2a359dba8bb1a6b447452ae0",
  agent_control_provider_authority_receipts:
    "1867a5e40e831c6b798a23cd93847a55cf7e1ea4260c12f8c1eece6aa96570e7",
  agent_control_provider_authority_receipts_no_delete:
    "6de3a6a5defa679db96bd1c65f6adca70ffbbe0bd66f3fcd6359b01af442025c",
  agent_control_provider_authority_receipts_no_update:
    "2fd219c3c6e3ff9ee4c8b4eff1d31c5476e0affe8e30b15e1288ba426b58bb7e",
  agent_control_provider_capacity_current:
    "bd078ed15f8e3d9d36e6e599ec1a6323f1ba72b4a5bc837de31019120bcd35f3",
  agent_control_provider_capacity_current_validate_insert:
    "15db37d9b75b2e54529c76b060ec275986c5fe028f6b578ebad8b16681bf9009",
  agent_control_provider_capacity_current_validate_update:
    "e9ee1f937a60e00bea949f33f4ee3eb00e214a8201da654e2e0b41a56e34f456",
  agent_control_provider_capacity_current_no_delete:
    "678fb6431a40ac2159ef744a5af00248a0370beaa5e0ae9a3d52a56c9a87980b",
  agent_control_provider_claim_history:
    "904e69ba21fce57a4e0b6f0a6da45f8f02267892e4b3f8c47287d088a2625e9d",
  agent_control_provider_claim_history_no_delete:
    "23b247a7f113beadece739662d57037c5a0db874ad7d1546cdee204a09674eeb",
  agent_control_provider_claim_history_no_update:
    "2a8a9099f43be809ab97c3e2eb03140454ede76759018a006410a7285d431c69",
  agent_control_provider_usage_evidence:
    "399be3ebf27ec8d1ca67465e023e66f2e05f9adaf4d4fc40350581364d568860",
  agent_control_provider_usage_evidence_no_delete:
    "3c7394093e4151a9eb40a9616c162f77359c20f726d46f12aa28dd19da607c14",
  agent_control_provider_usage_evidence_no_update:
    "187975c03e505ca17e68d2db97a522ef957165baef9b25c29ed841accbee1755",
  agent_control_provider_usage_evidence_validate:
    "940b432bee33fe4c6ef7e29d6a3aa9bd3359455b20a5347e5735786614a7c086",
  idx_agent_control_provider_admission_deadline:
    "a3a37248a42803b1e9d25da366d61e00ffcf1769de93ce5bb76d977c0ac197fb",
  idx_agent_control_provider_admission_lease_deadline:
    "651ee3c9d163ed597c2b38ec1d9529b582c731421fa6c1f0f4c7149aaa9e07ae",
  idx_agent_control_provider_admission_queue:
    "00b31f8aaf190823a28af7f9fb50cf94e2ae6b26ee29aef952c663d099ad635c",
  idx_agent_control_provider_authority_admission:
    "36f61f715b7c612274149781b5888592c919fe5c51abe1b46118bb0f9a221e99",
};

export const makeMigration065 = (
  injectFault: (point: Migration065FaultPoint) => Effect.Effect<void> = () => Effect.void,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const collisions = yield* sql<{ readonly name: string }>`
      SELECT name FROM main.sqlite_schema
      WHERE name IN ${sql.in(PROVIDER_ADMISSION_SCHEMA_OBJECTS)}
      ORDER BY name
    `;
    if (collisions.length !== 0) {
      return yield* Effect.die(new Error("migration 065 encountered partial or divergent schema"));
    }
    const udf = yield* sql<{
      readonly positive: number;
      readonly textNegative: number;
      readonly bytesNegative: number;
    }>`
      SELECT
        ${sql.literal(CANONICAL_BLOB_MATCH)}(CAST('{"schemaVersion":1}' AS BLOB), '0e9561cfb83d50990a103b3896fe249a11fe27fa28985448187f93ec12116d72') AS positive,
        ${sql.literal(CANONICAL_BLOB_MATCH)}('{"schemaVersion":1}', '0e9561cfb83d50990a103b3896fe249a11fe27fa28985448187f93ec12116d72') AS textNegative,
        ${sql.literal(CANONICAL_BLOB_MATCH)}(CAST('{"schemaVersion":2}' AS BLOB), '0e9561cfb83d50990a103b3896fe249a11fe27fa28985448187f93ec12116d72') AS bytesNegative
    `;
    if (
      udf.length !== 1 ||
      udf[0]?.positive !== 1 ||
      udf[0].textNegative !== 0 ||
      udf[0].bytesNegative !== 0
    ) {
      return yield* Effect.die(new Error("migration 065 requires canonical admission UDFs"));
    }
    yield* sql`PRAGMA defer_foreign_keys=ON`;
    yield* createTables;
    yield* injectFault("after-tables");
    yield* createIndexes;
    yield* injectFault("after-indexes");
    yield* createAppendOnlyTriggers;
    yield* createValidationTriggers;
    yield* injectFault("after-triggers");
    const objects = yield* sql<{ readonly name: string; readonly sql: string }>`
      SELECT name, sql FROM main.sqlite_schema
      WHERE name IN ${sql.in(PROVIDER_ADMISSION_SCHEMA_OBJECTS)} AND sql IS NOT NULL
      ORDER BY name
    `;
    if (objects.length !== PROVIDER_ADMISSION_SCHEMA_OBJECTS.length) {
      return yield* Effect.die(new Error("migration 065 schema creation is incomplete"));
    }
    if (
      Object.keys(EXPECTED_PROVIDER_ADMISSION_DDL_FINGERPRINTS).length !== objects.length ||
      objects.some(
        (object) =>
          EXPECTED_PROVIDER_ADMISSION_DDL_FINGERPRINTS[object.name] !== sha256Utf8(object.sql),
      )
    ) {
      const mismatches = objects
        .filter(
          (object) =>
            EXPECTED_PROVIDER_ADMISSION_DDL_FINGERPRINTS[object.name] !== sha256Utf8(object.sql),
        )
        .map((object) => `${object.name}=${sha256Utf8(object.sql)}`)
        .join(",");
      return yield* Effect.die(new Error(`migration 065 DDL fingerprint mismatch: ${mismatches}`));
    }
    const foreignKeys = yield* sql<Record<string, unknown>>`PRAGMA main.foreign_key_check`;
    if (foreignKeys.length !== 0) {
      return yield* Effect.die(new Error("migration 065 introduced foreign-key violations"));
    }
    const integrity = yield* sql<{ readonly integrity_check: string }>`PRAGMA main.integrity_check`;
    if (integrity.length !== 1 || integrity[0]?.integrity_check !== "ok") {
      return yield* Effect.die(new Error("migration 065 failed integrity_check"));
    }
    yield* sql`PRAGMA defer_foreign_keys=OFF`;
  });

export default makeMigration065();
