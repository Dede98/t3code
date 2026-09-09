import { ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  sha256Utf8,
  type JsonValue,
} from "../../initialPlanning/eventEvidence.ts";
import { decodePersistedOrchestrationMetadata } from "../../../orchestration/providerRuntimeMessageCorrelation.ts";
import {
  EXPECTED_PROVIDER_ADMISSION_DDL_FINGERPRINTS,
  PROVIDER_ADMISSION_SCHEMA_OBJECTS,
} from "../../../persistence/Migrations/065_AgentControlProviderCapacityAdmission.ts";
import {
  fingerprintProviderAdmissionDocument,
  providerAdmissionAuthorityId,
  providerAdmissionId,
  type ProviderAdmissionDecision,
  type ProviderAdmissionPermit,
  type ProviderAdmissionRequest,
  type ProviderAdmissionStage,
  type ProviderAdmissionUsageEvidence,
  usageAllowsAdmission,
} from "../model.ts";
import {
  ProviderAdmissionError,
  ProviderAdmissionStore,
  type ProviderAdmissionDeadlineWakeup,
  type ProviderAdmissionStoreShape,
  type ProviderAdmissionWakeup,
} from "../Services/ProviderAdmissionStore.ts";

const utf8 = (value: string): Uint8Array => new TextEncoder().encode(value);
const isProviderAdmissionError = Schema.is(ProviderAdmissionError);

const fail = (
  operation: string,
  reason: ProviderAdmissionError["reason"],
  admissionId?: string,
  cause?: unknown,
) => new ProviderAdmissionError({ operation, reason, admissionId, cause });

export const PROVIDER_ADMISSION_OLDEST_ELIGIBLE_SQL = `
SELECT admission_id AS "admissionId"
FROM main.agent_control_provider_admission_current
INDEXED BY idx_agent_control_provider_admission_queue
WHERE provider_instance_id = ?
  AND typeof(provider_instance_id) = 'text'
  AND status = 'waiting'
  AND typeof(status) = 'text'
  AND usage_eligible = 1
  AND typeof(usage_eligible) = 'integer'
  AND typeof(requested_at) = 'text'
  AND typeof(admission_id) = 'text'
ORDER BY requested_at, admission_id
LIMIT 1
`.trim();

export const PROVIDER_ADMISSION_MINIMUM_WAITING_DEADLINE_SQL = `
SELECT next_deadline_at AS deadline
FROM main.agent_control_provider_admission_current
INDEXED BY idx_agent_control_provider_admission_deadline
WHERE status = 'waiting'
  AND typeof(status) = 'text'
  AND next_deadline_at IS NOT NULL
  AND typeof(next_deadline_at) = 'text'
ORDER BY next_deadline_at, provider_instance_id, admission_id
LIMIT 1
`.trim();

export const PROVIDER_ADMISSION_MINIMUM_ADMITTED_DEADLINE_SQL = `
SELECT lease_expires_at AS deadline
FROM main.agent_control_provider_admission_current
INDEXED BY idx_agent_control_provider_admission_lease_deadline
WHERE status = 'admitted'
  AND typeof(status) = 'text'
  AND lease_expires_at IS NOT NULL
  AND typeof(lease_expires_at) = 'text'
ORDER BY lease_expires_at, provider_instance_id, admission_id
LIMIT 1
`.trim();

export const PROVIDER_ADMISSION_MINIMUM_WAITING_DEADLINE_AFTER_SQL = `
SELECT next_deadline_at AS deadline
FROM main.agent_control_provider_admission_current
INDEXED BY idx_agent_control_provider_admission_deadline
WHERE status = 'waiting'
  AND typeof(status) = 'text'
  AND next_deadline_at IS NOT NULL
  AND typeof(next_deadline_at) = 'text'
  AND next_deadline_at > ?
ORDER BY next_deadline_at, provider_instance_id, admission_id
LIMIT 1
`.trim();

export const PROVIDER_ADMISSION_MINIMUM_ADMITTED_DEADLINE_AFTER_SQL = `
SELECT lease_expires_at AS deadline
FROM main.agent_control_provider_admission_current
INDEXED BY idx_agent_control_provider_admission_lease_deadline
WHERE status = 'admitted'
  AND typeof(status) = 'text'
  AND lease_expires_at IS NOT NULL
  AND typeof(lease_expires_at) = 'text'
  AND lease_expires_at > ?
ORDER BY lease_expires_at, provider_instance_id, admission_id
LIMIT 1
`.trim();

export const PROVIDER_ADMISSION_DUE_WAITING_DEADLINES_SQL = `
SELECT admission_id AS "admissionId",stage,handoff_id AS "handoffId",
  provider_instance_id AS "providerInstanceId",next_deadline_at AS "deadlineAt",
  'usage' AS "deadlineKind"
FROM main.agent_control_provider_admission_current
INDEXED BY idx_agent_control_provider_admission_deadline
WHERE status = 'waiting'
  AND typeof(status) = 'text'
  AND next_deadline_at IS NOT NULL
  AND typeof(next_deadline_at) = 'text'
  AND next_deadline_at <= ?
ORDER BY next_deadline_at, provider_instance_id, admission_id
`.trim();

export const PROVIDER_ADMISSION_DUE_ADMITTED_DEADLINES_SQL = `
SELECT admission_id AS "admissionId",stage,handoff_id AS "handoffId",
  provider_instance_id AS "providerInstanceId",lease_expires_at AS "deadlineAt",
  'lease' AS "deadlineKind"
FROM main.agent_control_provider_admission_current
INDEXED BY idx_agent_control_provider_admission_lease_deadline
WHERE status = 'admitted'
  AND typeof(status) = 'text'
  AND lease_expires_at IS NOT NULL
  AND typeof(lease_expires_at) = 'text'
  AND lease_expires_at <= ?
ORDER BY lease_expires_at, provider_instance_id, admission_id
`.trim();

interface CurrentRow {
  readonly admissionId: string;
  readonly providerInstanceId: string;
  readonly stage: ProviderAdmissionStage;
  readonly handoffId: string;
  readonly status: string;
  readonly requestedAt: string;
  readonly usageStatus: ProviderAdmissionUsageEvidence["status"];
  readonly usageEvidenceFingerprint: string;
  readonly nextDeadlineAt: string | null;
  readonly ownerId: string | null;
  readonly leaseExpiresAt: string | null;
  readonly providerFenceToken: number | null;
  readonly admissionMarkerId: string | null;
  readonly admissionMarkerFingerprint: string | null;
}

interface IntentRow {
  readonly admissionId: string;
  readonly stage: ProviderAdmissionStage;
  readonly projectId: string;
  readonly taskId: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly handoffId: string;
  readonly providerDeliveryId: string;
  readonly threadId: string;
  readonly providerInstanceId: string;
  readonly stageLeaseId: string;
  readonly stageLeaseHolderId: string;
  readonly stageFenceToken: number;
  readonly modelSelectionJson: unknown;
  readonly modelSelectionFingerprint: string;
  readonly requestedAt: string;
  readonly intentFingerprint: string;
}

interface CapacityRow {
  readonly providerInstanceId: string;
  readonly lastFenceToken: number;
  readonly activeAdmissionId: string | null;
  readonly activeState: string | null;
  readonly activeOwnerId: string | null;
  readonly activeLeaseExpiresAt: string | null;
  readonly activeFenceToken: number | null;
  readonly activeMarkerFingerprint: string | null;
}

interface ClaimRow {
  readonly claimId: string;
  readonly admissionId: string;
  readonly providerInstanceId: string;
  readonly ownerId: string;
  readonly providerFenceToken: number;
  readonly claimedAt: string;
  readonly leaseExpiresAt: string;
  readonly claimFingerprint: string;
}

interface FinalizationRow {
  readonly handoffId: string;
  readonly projectId: string;
  readonly taskId: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly fenceToken: number;
  readonly providerDeliveryId: string;
  readonly providerInstanceId: string;
  readonly terminalRuntimeEventId: string | null;
  readonly terminalOrchestrationEventId: string;
  readonly terminalEventType: string;
  readonly terminalStreamVersion: number;
  readonly terminalMetadataStorageClass: unknown;
  readonly terminalMetadataBytes: unknown;
  readonly terminalMetadataText: unknown;
  readonly finalizationMarkerId: string;
  readonly finalizationMarkerFingerprint: string;
  readonly finalizedAt: string;
}

interface AuthorityChainRow {
  readonly authorityKind:
    | "admission"
    | "session-entry"
    | "turn-entry"
    | "quarantine"
    | "supersede"
    | "release";
  readonly providerInstanceId: string;
  readonly ownerId: string;
  readonly providerFenceToken: number;
  readonly occurredAt: string;
  readonly terminalRuntimeEventId: string | null;
  readonly terminalEventType: string | null;
  readonly terminalStreamVersion: number | null;
  readonly finalizationMarkerId: string | null;
  readonly finalizationMarkerFingerprint: string | null;
  readonly payloadStorageClass: unknown;
  readonly payloadBytes: unknown;
  readonly payloadFingerprint: string;
  readonly evidenceId: string;
  readonly receiptId: string;
  readonly markerId: string;
  readonly markerFingerprint: string;
  readonly committedAt: string;
  readonly markerSequence: number;
}

interface ProviderAdmissionAttemptInput {
  readonly request: ProviderAdmissionRequest;
  readonly usage?: ProviderAdmissionUsageEvidence;
  readonly ownerId: string;
  readonly leaseExpiresAt: string;
  readonly now: string;
}

const intentDocument = (admissionId: string, request: ProviderAdmissionRequest) =>
  ({
    admissionId,
    attemptId: request.attemptId,
    handoffId: request.handoffId,
    modelSelectionFingerprint: request.modelSelectionFingerprint,
    projectId: request.projectId,
    providerDeliveryId: request.providerDeliveryId,
    providerInstanceId: String(request.providerInstanceId),
    requestedAt: request.requestedAt,
    schemaVersion: 1,
    stage: request.stage,
    stageFenceToken: request.stageFenceToken,
    stageLeaseHolderId: request.stageLeaseHolderId,
    stageLeaseId: request.stageLeaseId,
    stageRunId: request.stageRunId,
    taskId: request.taskId,
    threadId: String(request.threadId),
  }) as const;

const usageDocument = (
  admissionId: string,
  providerInstanceId: string,
  usage: ProviderAdmissionUsageEvidence,
) =>
  ({
    admissionId,
    nextRelevantAt: usage.nextRelevantAt,
    observedAt: usage.observedAt,
    providerInstanceId,
    schemaVersion: 1,
    source: usage.source,
    status: usage.status,
  }) as const;

const usageEvidenceId = (admissionId: string, fingerprint: string) =>
  `provider-usage:${sha256Utf8(canonicalJson([admissionId, fingerprint]))}`;

const claimId = (admissionId: string, ownerId: string, fence: number) =>
  `provider-claim:${sha256Utf8(canonicalJson([admissionId, ownerId, fence]))}`;

const authorityDocument = (input: {
  readonly admissionId: string;
  readonly authorityKind:
    | "admission"
    | "session-entry"
    | "turn-entry"
    | "quarantine"
    | "supersede"
    | "release";
  readonly providerInstanceId: string;
  readonly ownerId: string;
  readonly providerFenceToken: number;
  readonly occurredAt: string;
  readonly details: Record<string, JsonValue>;
}) =>
  ({
    admissionId: input.admissionId,
    authorityKind: input.authorityKind,
    occurredAt: input.occurredAt,
    ownerId: input.ownerId,
    providerFenceToken: input.providerFenceToken,
    providerInstanceId: input.providerInstanceId,
    schemaVersion: 1,
    ...input.details,
  }) as JsonValue;

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const readCurrent = (admissionId: string) =>
    sql<CurrentRow>`
      SELECT admission_id AS "admissionId", provider_instance_id AS "providerInstanceId",
        stage, handoff_id AS "handoffId", status, requested_at AS "requestedAt",
        usage_status AS "usageStatus", usage_evidence_fingerprint AS "usageEvidenceFingerprint",
        next_deadline_at AS "nextDeadlineAt",
        owner_id AS "ownerId", lease_expires_at AS "leaseExpiresAt",
        provider_fence_token AS "providerFenceToken", admission_marker_id AS "admissionMarkerId",
        admission_marker_fingerprint AS "admissionMarkerFingerprint"
      FROM main.agent_control_provider_admission_current
      WHERE admission_id=${admissionId}
        AND typeof(admission_id)='text' AND typeof(provider_instance_id)='text'
        AND typeof(stage)='text' AND typeof(handoff_id)='text' AND typeof(status)='text'
    `.pipe(Effect.map((rows) => rows[0]));

  const readIntent = (admissionId: string) =>
    sql<IntentRow>`
      SELECT admission_id AS "admissionId", stage, project_id AS "projectId",
        task_id AS "taskId", stage_run_id AS "stageRunId", attempt_id AS "attemptId",
        handoff_id AS "handoffId", provider_delivery_id AS "providerDeliveryId",
        thread_id AS "threadId", provider_instance_id AS "providerInstanceId",
        stage_lease_id AS "stageLeaseId", stage_lease_holder_id AS "stageLeaseHolderId",
        stage_fence_token AS "stageFenceToken", model_selection_json AS "modelSelectionJson",
        model_selection_fingerprint AS "modelSelectionFingerprint", requested_at AS "requestedAt",
        intent_fingerprint AS "intentFingerprint"
      FROM main.agent_control_provider_admission_intents
      WHERE admission_id=${admissionId}
        AND typeof(admission_id)='text' AND typeof(stage)='text'
        AND typeof(project_id)='text' AND typeof(task_id)='text'
        AND typeof(stage_run_id)='text' AND typeof(attempt_id)='text'
        AND typeof(handoff_id)='text' AND typeof(provider_delivery_id)='text'
        AND typeof(thread_id)='text' AND typeof(provider_instance_id)='text'
        AND typeof(stage_lease_id)='text' AND typeof(stage_lease_holder_id)='text'
        AND typeof(stage_fence_token)='integer' AND typeof(model_selection_json)='blob'
        AND typeof(model_selection_fingerprint)='text' AND typeof(requested_at)='text'
        AND typeof(intent_fingerprint)='text'
    `.pipe(Effect.map((rows) => rows[0]));

  const readCapacity = (providerInstanceId: string) =>
    sql<CapacityRow>`
      SELECT provider_instance_id AS "providerInstanceId", last_fence_token AS "lastFenceToken",
        active_admission_id AS "activeAdmissionId", active_state AS "activeState",
        active_owner_id AS "activeOwnerId", active_lease_expires_at AS "activeLeaseExpiresAt",
        active_fence_token AS "activeFenceToken", active_marker_fingerprint AS "activeMarkerFingerprint"
      FROM main.agent_control_provider_capacity_current
      WHERE provider_instance_id=${providerInstanceId} AND typeof(provider_instance_id)='text'
        AND typeof(last_fence_token)='integer'
    `.pipe(Effect.map((rows) => rows[0]));

  const appendAuthority = Effect.fn("ProviderAdmissionStore.appendAuthority")(function* (input: {
    readonly admissionId: string;
    readonly authorityKind:
      | "admission"
      | "session-entry"
      | "turn-entry"
      | "quarantine"
      | "supersede"
      | "release";
    readonly providerInstanceId: string;
    readonly ownerId: string;
    readonly providerFenceToken: number;
    readonly occurredAt: string;
    readonly details?: Record<string, JsonValue>;
    readonly terminalRuntimeEventId?: string;
    readonly terminalEventType?: string;
    readonly terminalStreamVersion?: number;
    readonly finalizationMarkerId?: string;
    readonly finalizationMarkerFingerprint?: string;
  }) {
    const existing = yield* sql<{
      readonly markerId: string;
      readonly markerFingerprint: string;
      readonly ownerId: string;
      readonly providerFenceToken: number;
    }>`
      SELECT marker.marker_id AS "markerId", marker.marker_fingerprint AS "markerFingerprint",
        evidence.owner_id AS "ownerId", evidence.provider_fence_token AS "providerFenceToken"
      FROM main.agent_control_provider_authority_markers marker
      JOIN main.agent_control_provider_authority_evidence evidence
        ON evidence.evidence_id=marker.evidence_id
      WHERE marker.admission_id=${input.admissionId}
        AND marker.authority_kind=${input.authorityKind}
        AND evidence.provider_fence_token=${input.providerFenceToken}
    `;
    const document = authorityDocument({
      admissionId: input.admissionId,
      authorityKind: input.authorityKind,
      providerInstanceId: input.providerInstanceId,
      ownerId: input.ownerId,
      providerFenceToken: input.providerFenceToken,
      occurredAt: input.occurredAt,
      details: input.details ?? {},
    });
    const fingerprint = fingerprintProviderAdmissionDocument(document);
    const evidenceId = providerAdmissionAuthorityId(
      input.authorityKind,
      input.admissionId,
      `${input.providerFenceToken}:evidence`,
    );
    const receiptId = providerAdmissionAuthorityId(
      input.authorityKind,
      input.admissionId,
      `${input.providerFenceToken}:receipt`,
    );
    const markerId = providerAdmissionAuthorityId(
      input.authorityKind,
      input.admissionId,
      `${input.providerFenceToken}:marker`,
    );
    if (existing.length !== 0) {
      const row = existing[0]!;
      if (
        existing.length !== 1 ||
        row.markerId !== markerId ||
        row.markerFingerprint !== fingerprint ||
        row.ownerId !== input.ownerId ||
        row.providerFenceToken !== input.providerFenceToken
      ) {
        return yield* fail("append-authority-replay", "authority-divergent", input.admissionId);
      }
      return { markerId, markerFingerprint: fingerprint };
    }
    yield* sql`
      INSERT INTO main.agent_control_provider_authority_evidence (
        evidence_id,receipt_id,marker_id,admission_id,authority_kind,provider_instance_id,
        owner_id,provider_fence_token,occurred_at,terminal_runtime_event_id,terminal_event_type,
        terminal_stream_version,finalization_marker_id,finalization_marker_fingerprint,
        payload_json,payload_fingerprint
      ) VALUES (
        ${evidenceId},${receiptId},${markerId},${input.admissionId},${input.authorityKind},
        ${input.providerInstanceId},${input.ownerId},${input.providerFenceToken},${input.occurredAt},
        ${input.terminalRuntimeEventId ?? null},${input.terminalEventType ?? null},
        ${input.terminalStreamVersion ?? null},${input.finalizationMarkerId ?? null},
        ${input.finalizationMarkerFingerprint ?? null},${utf8(canonicalJson(document))},${fingerprint}
      )
    `;
    yield* sql`
      INSERT INTO main.agent_control_provider_authority_receipts (
        receipt_id,evidence_id,marker_id,admission_id,authority_kind,status,accepted_at
      ) VALUES (${receiptId},${evidenceId},${markerId},${input.admissionId},${input.authorityKind},'accepted',${input.occurredAt})
    `;
    yield* sql`
      INSERT INTO main.agent_control_provider_authority_markers (
        marker_id,evidence_id,receipt_id,admission_id,authority_kind,marker_fingerprint,committed_at
      ) VALUES (${markerId},${evidenceId},${receiptId},${input.admissionId},${input.authorityKind},${fingerprint},${input.occurredAt})
    `;
    return { markerId, markerFingerprint: fingerprint };
  });

  const readCompleteAuthorityChains = (admissionId: string) =>
    sql<AuthorityChainRow>`
      SELECT evidence.authority_kind AS "authorityKind",
        evidence.provider_instance_id AS "providerInstanceId",
        evidence.owner_id AS "ownerId",
        evidence.provider_fence_token AS "providerFenceToken",
        evidence.occurred_at AS "occurredAt",
        evidence.terminal_runtime_event_id AS "terminalRuntimeEventId",
        evidence.terminal_event_type AS "terminalEventType",
        evidence.terminal_stream_version AS "terminalStreamVersion",
        evidence.finalization_marker_id AS "finalizationMarkerId",
        evidence.finalization_marker_fingerprint AS "finalizationMarkerFingerprint",
        typeof(evidence.payload_json) AS "payloadStorageClass",
        CAST(evidence.payload_json AS BLOB) AS "payloadBytes",
        evidence.payload_fingerprint AS "payloadFingerprint",
        evidence.evidence_id AS "evidenceId",receipt.receipt_id AS "receiptId",
        marker.marker_id AS "markerId",marker.marker_fingerprint AS "markerFingerprint",
        marker.committed_at AS "committedAt",marker.rowid AS "markerSequence"
      FROM main.agent_control_provider_authority_evidence evidence
      JOIN main.agent_control_provider_authority_receipts receipt
        ON receipt.receipt_id=evidence.receipt_id
        AND receipt.evidence_id=evidence.evidence_id
        AND receipt.marker_id=evidence.marker_id
        AND receipt.admission_id=evidence.admission_id
        AND receipt.authority_kind=evidence.authority_kind
        AND receipt.status='accepted'
        AND receipt.accepted_at=evidence.occurred_at
      JOIN main.agent_control_provider_authority_markers marker
        ON marker.marker_id=evidence.marker_id
        AND marker.evidence_id=evidence.evidence_id
        AND marker.receipt_id=evidence.receipt_id
        AND marker.admission_id=evidence.admission_id
        AND marker.authority_kind=evidence.authority_kind
        AND marker.marker_fingerprint=evidence.payload_fingerprint
        AND marker.committed_at=evidence.occurred_at
      WHERE evidence.admission_id=${admissionId}
        AND typeof(evidence.admission_id)='text'
        AND typeof(evidence.authority_kind)='text'
        AND typeof(evidence.provider_instance_id)='text'
        AND typeof(evidence.owner_id)='text'
        AND typeof(evidence.provider_fence_token)='integer'
        AND typeof(evidence.occurred_at)='text'
        AND typeof(evidence.payload_json)='blob'
        AND typeof(evidence.payload_fingerprint)='text'
        AND typeof(receipt.receipt_id)='text'
        AND typeof(receipt.status)='text'
        AND typeof(receipt.accepted_at)='text'
        AND typeof(marker.marker_id)='text'
        AND typeof(marker.marker_fingerprint)='text'
        AND typeof(marker.committed_at)='text'
      ORDER BY marker.committed_at,marker.marker_id
    `;

  const authorityChainMatches = (
    chain: AuthorityChainRow,
    input: {
      readonly admissionId: string;
      readonly authorityKind: AuthorityChainRow["authorityKind"];
      readonly providerInstanceId: string;
      readonly ownerId: string;
      readonly providerFenceToken: number;
      readonly details: Record<string, JsonValue>;
    },
  ): boolean => {
    if (
      chain.authorityKind !== input.authorityKind ||
      chain.providerInstanceId !== input.providerInstanceId ||
      chain.ownerId !== input.ownerId ||
      chain.providerFenceToken !== input.providerFenceToken ||
      chain.payloadStorageClass !== "blob" ||
      chain.evidenceId !==
        providerAdmissionAuthorityId(
          input.authorityKind,
          input.admissionId,
          `${input.providerFenceToken}:evidence`,
        ) ||
      chain.receiptId !==
        providerAdmissionAuthorityId(
          input.authorityKind,
          input.admissionId,
          `${input.providerFenceToken}:receipt`,
        ) ||
      chain.markerId !==
        providerAdmissionAuthorityId(
          input.authorityKind,
          input.admissionId,
          `${input.providerFenceToken}:marker`,
        ) ||
      chain.committedAt !== chain.occurredAt
    ) {
      return false;
    }
    try {
      const payload = decodeCanonicalUtf8Bytes(chain.payloadBytes);
      const expected = canonicalJson(
        authorityDocument({
          admissionId: input.admissionId,
          authorityKind: input.authorityKind,
          providerInstanceId: input.providerInstanceId,
          ownerId: input.ownerId,
          providerFenceToken: input.providerFenceToken,
          occurredAt: chain.occurredAt,
          details: input.details,
        }),
      );
      return (
        payload === expected &&
        sha256Utf8(payload) === chain.payloadFingerprint &&
        chain.markerFingerprint === chain.payloadFingerprint
      );
    } catch {
      return false;
    }
  };

  const permitFromRows = (
    current: CurrentRow,
    intent: IntentRow,
  ): ProviderAdmissionPermit | undefined => {
    if (
      current.status !== "admitted" ||
      current.ownerId === null ||
      current.leaseExpiresAt === null ||
      current.providerFenceToken === null ||
      current.admissionMarkerId === null ||
      current.admissionMarkerFingerprint === null
    ) {
      return undefined;
    }
    return {
      admissionId: current.admissionId,
      admissionMarkerId: current.admissionMarkerId,
      admissionMarkerFingerprint: current.admissionMarkerFingerprint,
      stage: intent.stage,
      projectId: intent.projectId,
      taskId: intent.taskId,
      stageRunId: intent.stageRunId,
      attemptId: intent.attemptId,
      handoffId: intent.handoffId,
      providerDeliveryId: intent.providerDeliveryId,
      threadId: intent.threadId,
      providerInstanceId: ProviderInstanceId.make(intent.providerInstanceId),
      stageLeaseId: intent.stageLeaseId,
      stageLeaseHolderId: intent.stageLeaseHolderId,
      stageFenceToken: intent.stageFenceToken,
      admissionOwnerId: current.ownerId,
      admissionLeaseExpiresAt: current.leaseExpiresAt,
      providerFenceToken: current.providerFenceToken,
      modelSelectionJson: decodeCanonicalUtf8Bytes(intent.modelSelectionJson),
      modelSelectionFingerprint: intent.modelSelectionFingerprint,
      usageEvidenceFingerprint: current.usageEvidenceFingerprint,
    };
  };

  const requestInternal = Effect.fn("ProviderAdmissionStore.requestInternal")(function* (
    input: ProviderAdmissionAttemptInput,
  ) {
    const admissionId = providerAdmissionId(input.request);
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const expectedIntent = intentDocument(admissionId, input.request);
          const expectedIntentJson = canonicalJson(expectedIntent);
          const expectedIntentFingerprint = sha256Utf8(expectedIntentJson);
          let current = yield* readCurrent(admissionId);
          if (current === undefined) {
            if (input.usage === undefined) return null;
            yield* sql`
            INSERT INTO main.agent_control_provider_admission_intents (
              admission_id,stage,project_id,task_id,stage_run_id,attempt_id,handoff_id,
              provider_delivery_id,thread_id,provider_instance_id,stage_lease_id,
              stage_lease_holder_id,stage_fence_token,model_selection_json,
              model_selection_fingerprint,requested_at,intent_json,intent_fingerprint
            ) VALUES (
              ${admissionId},${input.request.stage},${input.request.projectId},${input.request.taskId},
              ${input.request.stageRunId},${input.request.attemptId},${input.request.handoffId},
              ${input.request.providerDeliveryId},${String(input.request.threadId)},
              ${String(input.request.providerInstanceId)},${input.request.stageLeaseId},
              ${input.request.stageLeaseHolderId},${input.request.stageFenceToken},
              ${utf8(input.request.modelSelectionJson)},${input.request.modelSelectionFingerprint},
              ${input.request.requestedAt},${utf8(expectedIntentJson)},${expectedIntentFingerprint}
            )
          `;
            const evidenceJson = canonicalJson(
              usageDocument(admissionId, String(input.request.providerInstanceId), input.usage),
            );
            const boundUsageFingerprint = sha256Utf8(evidenceJson);
            const evidenceId = usageEvidenceId(admissionId, boundUsageFingerprint);
            yield* sql`
            INSERT INTO main.agent_control_provider_usage_evidence (
              evidence_id,admission_id,provider_instance_id,status,source,observed_at,
              next_relevant_at,evidence_json,evidence_fingerprint
            ) VALUES (
              ${evidenceId},${admissionId},${String(input.request.providerInstanceId)},
              ${input.usage.status},${input.usage.source},${input.usage.observedAt},
              ${input.usage.nextRelevantAt},${utf8(evidenceJson)},${boundUsageFingerprint}
            )
          `;
            yield* sql`
            INSERT INTO main.agent_control_provider_admission_current (
              admission_id,provider_instance_id,stage,handoff_id,status,requested_at,
              usage_status,usage_eligible,usage_evidence_id,usage_evidence_fingerprint,
              next_deadline_at,owner_id,lease_expires_at,provider_fence_token,
              admission_marker_id,admission_marker_fingerprint,revision,updated_at
            ) VALUES (
              ${admissionId},${String(input.request.providerInstanceId)},${input.request.stage},
              ${input.request.handoffId},'waiting',${input.request.requestedAt},${input.usage.status},
              ${usageAllowsAdmission(input.usage.status) ? 1 : 0},${evidenceId},
              ${boundUsageFingerprint},${input.usage.nextRelevantAt},NULL,NULL,NULL,NULL,NULL,1,${input.now}
            )
          `;
            current = yield* readCurrent(admissionId);
          } else {
            const intent = yield* readIntent(admissionId);
            if (
              intent === undefined ||
              intent.intentFingerprint !== expectedIntentFingerprint ||
              intent.providerDeliveryId !== input.request.providerDeliveryId ||
              intent.modelSelectionFingerprint !== input.request.modelSelectionFingerprint
            ) {
              return yield* fail("request-replay", "authority-divergent", admissionId);
            }
          }
          if (current === undefined) {
            return yield* fail("request-current", "authority-missing", admissionId);
          }
          let capacity = yield* readCapacity(String(input.request.providerInstanceId));
          if (capacity === undefined) {
            yield* sql`
              INSERT INTO main.agent_control_provider_capacity_current (
                provider_instance_id,last_fence_token,active_admission_id,active_state,
                active_owner_id,active_lease_expires_at,active_fence_token,
                active_marker_fingerprint,revision,updated_at
              ) VALUES (${String(input.request.providerInstanceId)},0,NULL,NULL,NULL,NULL,NULL,NULL,1,${input.now})
            `;
            capacity = yield* readCapacity(String(input.request.providerInstanceId));
          }
          if (capacity === undefined) {
            return yield* fail("request-capacity", "authority-missing", admissionId);
          }
          if (current.status === "admitted") {
            if (current.leaseExpiresAt !== null && current.leaseExpiresAt > input.now) {
              if (current.ownerId !== input.ownerId) {
                return {
                  _tag: "Waiting",
                  admissionId,
                  retryAt: current.leaseExpiresAt,
                } satisfies ProviderAdmissionDecision;
              }
              const intent = yield* readIntent(admissionId);
              const permit = intent === undefined ? undefined : permitFromRows(current, intent);
              if (permit === undefined) {
                return yield* fail("request-permit", "authority-divergent", admissionId);
              }
              return { _tag: "Admitted", permit } satisfies ProviderAdmissionDecision;
            }
          }
          if (
            current.status === "entered" ||
            current.status === "quarantined" ||
            current.status === "released" ||
            current.status === "superseded"
          ) {
            return {
              _tag: "Waiting",
              admissionId,
              retryAt: null,
            } satisfies ProviderAdmissionDecision;
          }
          if (!usageAllowsAdmission(current.usageStatus)) {
            return {
              _tag: "Waiting",
              admissionId,
              retryAt: current.nextDeadlineAt,
            } satisfies ProviderAdmissionDecision;
          }
          const takeoverAdmitted = current.status === "admitted";
          let fence: number;
          if (capacity.activeAdmissionId !== null) {
            if (
              capacity.activeAdmissionId !== admissionId ||
              capacity.activeState === "entered" ||
              capacity.activeState === "quarantined" ||
              (capacity.activeState === "admitted" && !takeoverAdmitted) ||
              capacity.activeLeaseExpiresAt === null ||
              capacity.activeLeaseExpiresAt > input.now
            ) {
              return {
                _tag: "Waiting",
                admissionId,
                retryAt: null,
              } satisfies ProviderAdmissionDecision;
            }
            fence = capacity.lastFenceToken + 1;
          } else {
            const candidates = yield* sql.unsafe<{ readonly admissionId: string }>(
              PROVIDER_ADMISSION_OLDEST_ELIGIBLE_SQL,
              [String(input.request.providerInstanceId)],
            );
            if (candidates.length !== 1 || candidates[0]?.admissionId !== admissionId) {
              return {
                _tag: "Waiting",
                admissionId,
                retryAt: null,
              } satisfies ProviderAdmissionDecision;
            }
            fence = capacity.lastFenceToken + 1;
          }
          const claimFingerprint = sha256Utf8(
            canonicalJson([admissionId, input.ownerId, fence, input.now, input.leaseExpiresAt]),
          );
          yield* sql`
          INSERT INTO main.agent_control_provider_claim_history (
            claim_id,admission_id,provider_instance_id,owner_id,provider_fence_token,
            claimed_at,lease_expires_at,claim_fingerprint
          ) VALUES (${claimId(admissionId, input.ownerId, fence)},${admissionId},
            ${String(input.request.providerInstanceId)},${input.ownerId},${fence},${input.now},
            ${input.leaseExpiresAt},${claimFingerprint})
        `;
          if (!takeoverAdmitted) {
            yield* sql`
            UPDATE main.agent_control_provider_admission_current SET
              status='claimed',owner_id=${input.ownerId},lease_expires_at=${input.leaseExpiresAt},
              provider_fence_token=${fence},revision=revision+1,updated_at=${input.now}
            WHERE admission_id=${admissionId}
          `;
            yield* sql`
            UPDATE main.agent_control_provider_capacity_current SET
              last_fence_token=${fence},active_admission_id=${admissionId},active_state='claimed',
              active_owner_id=${input.ownerId},active_lease_expires_at=${input.leaseExpiresAt},
              active_fence_token=${fence},active_marker_fingerprint=NULL,
              revision=revision+1,updated_at=${input.now}
            WHERE provider_instance_id=${String(input.request.providerInstanceId)}
          `;
          }
          const marker = yield* appendAuthority({
            admissionId,
            authorityKind: "admission",
            providerInstanceId: String(input.request.providerInstanceId),
            ownerId: input.ownerId,
            providerFenceToken: fence,
            occurredAt: input.now,
            details: {
              handoffId: input.request.handoffId,
              providerDeliveryId: input.request.providerDeliveryId,
              usageEvidenceFingerprint: current.usageEvidenceFingerprint,
            },
          });
          yield* sql`
          UPDATE main.agent_control_provider_admission_current SET
            status='admitted',owner_id=${input.ownerId},lease_expires_at=${input.leaseExpiresAt},
            provider_fence_token=${fence},admission_marker_id=${marker.markerId},
            admission_marker_fingerprint=${marker.markerFingerprint},revision=revision+1,
            updated_at=${input.now}
          WHERE admission_id=${admissionId}
        `;
          yield* sql`
          UPDATE main.agent_control_provider_capacity_current SET last_fence_token=${fence},
            active_admission_id=${admissionId},active_state='admitted',
            active_owner_id=${input.ownerId},active_lease_expires_at=${input.leaseExpiresAt},
            active_fence_token=${fence},active_marker_fingerprint=${marker.markerFingerprint},revision=revision+1,
            updated_at=${input.now}
          WHERE provider_instance_id=${String(input.request.providerInstanceId)}
        `;
          const admitted = yield* readCurrent(admissionId);
          const intent = yield* readIntent(admissionId);
          const permit =
            admitted === undefined || intent === undefined
              ? undefined
              : permitFromRows(admitted, intent);
          if (permit === undefined)
            return yield* fail("request-commit", "authority-divergent", admissionId);
          return { _tag: "Admitted", permit } satisfies ProviderAdmissionDecision;
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isProviderAdmissionError(cause)
            ? cause
            : fail("request", "persistence", admissionId, cause),
        ),
      );
  });

  const resume: ProviderAdmissionStoreShape["resume"] = (input) => requestInternal(input);
  const request: ProviderAdmissionStoreShape["request"] = (input) =>
    requestInternal(input).pipe(
      Effect.flatMap((decision) =>
        decision === null
          ? Effect.fail(
              fail(
                "request-created-decision",
                "authority-missing",
                providerAdmissionId(input.request),
              ),
            )
          : Effect.succeed(decision),
      ),
    );

  const admitOldest: ProviderAdmissionStoreShape["admitOldest"] = Effect.fn(
    "ProviderAdmissionStore.admitOldest",
  )(function* (input) {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          let capacity = yield* readCapacity(input.providerInstanceId);
          if (capacity === undefined) {
            yield* sql`
            INSERT INTO main.agent_control_provider_capacity_current (
              provider_instance_id,last_fence_token,active_admission_id,active_state,
              active_owner_id,active_lease_expires_at,active_fence_token,
              active_marker_fingerprint,revision,updated_at
            ) VALUES (${input.providerInstanceId},0,NULL,NULL,NULL,NULL,NULL,NULL,1,${input.now})
          `;
            capacity = yield* readCapacity(input.providerInstanceId);
          }
          if (capacity === undefined) {
            return yield* fail("admit-oldest-capacity", "authority-missing");
          }
          if (capacity.activeAdmissionId !== null) return null;
          const candidates = yield* sql.unsafe<{ readonly admissionId: string }>(
            PROVIDER_ADMISSION_OLDEST_ELIGIBLE_SQL,
            [input.providerInstanceId],
          );
          if (candidates.length === 0) return null;
          if (candidates.length !== 1) {
            return yield* fail("admit-oldest-candidate", "authority-divergent");
          }
          const admissionId = candidates[0]!.admissionId;
          const [current, intent] = yield* Effect.all([
            readCurrent(admissionId),
            readIntent(admissionId),
          ]);
          if (
            current === undefined ||
            intent === undefined ||
            current.status !== "waiting" ||
            !usageAllowsAdmission(current.usageStatus) ||
            current.providerInstanceId !== input.providerInstanceId ||
            intent.providerInstanceId !== input.providerInstanceId
          ) {
            return yield* fail("admit-oldest-authority", "authority-divergent", admissionId);
          }
          const fence = capacity.lastFenceToken + 1;
          const claimFingerprint = sha256Utf8(
            canonicalJson([admissionId, input.ownerId, fence, input.now, input.leaseExpiresAt]),
          );
          yield* sql`
          INSERT INTO main.agent_control_provider_claim_history (
            claim_id,admission_id,provider_instance_id,owner_id,provider_fence_token,
            claimed_at,lease_expires_at,claim_fingerprint
          ) VALUES (${claimId(admissionId, input.ownerId, fence)},${admissionId},
            ${input.providerInstanceId},${input.ownerId},${fence},${input.now},
            ${input.leaseExpiresAt},${claimFingerprint})
        `;
          yield* sql`
          UPDATE main.agent_control_provider_admission_current SET
            status='claimed',owner_id=${input.ownerId},lease_expires_at=${input.leaseExpiresAt},
            provider_fence_token=${fence},revision=revision+1,updated_at=${input.now}
          WHERE admission_id=${admissionId}
        `;
          yield* sql`
          UPDATE main.agent_control_provider_capacity_current SET
            last_fence_token=${fence},active_admission_id=${admissionId},active_state='claimed',
            active_owner_id=${input.ownerId},active_lease_expires_at=${input.leaseExpiresAt},
            active_fence_token=${fence},active_marker_fingerprint=NULL,
            revision=revision+1,updated_at=${input.now}
          WHERE provider_instance_id=${input.providerInstanceId}
            AND active_admission_id IS NULL
        `;
          const marker = yield* appendAuthority({
            admissionId,
            authorityKind: "admission",
            providerInstanceId: input.providerInstanceId,
            ownerId: input.ownerId,
            providerFenceToken: fence,
            occurredAt: input.now,
            details: {
              handoffId: intent.handoffId,
              providerDeliveryId: intent.providerDeliveryId,
              usageEvidenceFingerprint: current.usageEvidenceFingerprint,
            },
          });
          yield* sql`
          UPDATE main.agent_control_provider_admission_current SET
            status='admitted',owner_id=${input.ownerId},lease_expires_at=${input.leaseExpiresAt},
            provider_fence_token=${fence},admission_marker_id=${marker.markerId},
            admission_marker_fingerprint=${marker.markerFingerprint},revision=revision+1,
            updated_at=${input.now}
          WHERE admission_id=${admissionId}
        `;
          yield* sql`
          UPDATE main.agent_control_provider_capacity_current SET
            active_state='admitted',active_marker_fingerprint=${marker.markerFingerprint},
            revision=revision+1,updated_at=${input.now}
          WHERE provider_instance_id=${input.providerInstanceId}
            AND active_admission_id=${admissionId} AND active_fence_token=${fence}
        `;
          const admitted = yield* readCurrent(admissionId);
          const permit = admitted === undefined ? undefined : permitFromRows(admitted, intent);
          if (permit === undefined) {
            return yield* fail("admit-oldest-commit", "authority-divergent", admissionId);
          }
          return permit;
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isProviderAdmissionError(cause)
            ? cause
            : fail("admit-oldest", "persistence", undefined, cause),
        ),
      );
  });

  const validateAndEnterInTransactionRaw = Effect.fn(
    "ProviderAdmissionStore.validateAndEnterInTransaction",
  )(function* (input: {
    readonly permit: ProviderAdmissionPermit;
    readonly boundary: "session-start" | "turn-start";
    readonly enteredAt: string;
  }) {
    const [current, intent, capacity] = yield* Effect.all([
      readCurrent(input.permit.admissionId),
      readIntent(input.permit.admissionId),
      readCapacity(String(input.permit.providerInstanceId)),
    ]);
    if (current === undefined || intent === undefined || capacity === undefined) {
      return yield* fail("pre-effect-read", "authority-missing", input.permit.admissionId);
    }
    const exact =
      (current.status === "admitted" || current.status === "entered") &&
      current.providerInstanceId === String(input.permit.providerInstanceId) &&
      current.leaseExpiresAt !== null &&
      current.ownerId === input.permit.admissionOwnerId &&
      current.leaseExpiresAt === input.permit.admissionLeaseExpiresAt &&
      current.providerFenceToken === input.permit.providerFenceToken &&
      current.admissionMarkerId === input.permit.admissionMarkerId &&
      current.admissionMarkerFingerprint === input.permit.admissionMarkerFingerprint &&
      current.usageEvidenceFingerprint === input.permit.usageEvidenceFingerprint &&
      current.leaseExpiresAt > input.enteredAt &&
      capacity.activeAdmissionId === input.permit.admissionId &&
      (capacity.activeState === "admitted" || capacity.activeState === "entered") &&
      capacity.activeOwnerId === input.permit.admissionOwnerId &&
      capacity.activeLeaseExpiresAt === input.permit.admissionLeaseExpiresAt &&
      capacity.activeFenceToken === input.permit.providerFenceToken &&
      capacity.lastFenceToken === input.permit.providerFenceToken &&
      capacity.activeMarkerFingerprint === input.permit.admissionMarkerFingerprint &&
      intent.projectId === input.permit.projectId &&
      intent.taskId === input.permit.taskId &&
      intent.stageRunId === input.permit.stageRunId &&
      intent.attemptId === input.permit.attemptId &&
      intent.handoffId === input.permit.handoffId &&
      intent.providerDeliveryId === input.permit.providerDeliveryId &&
      intent.threadId === input.permit.threadId &&
      intent.stageLeaseId === input.permit.stageLeaseId &&
      intent.stageLeaseHolderId === input.permit.stageLeaseHolderId &&
      intent.stageFenceToken === input.permit.stageFenceToken &&
      intent.modelSelectionFingerprint === input.permit.modelSelectionFingerprint &&
      decodeCanonicalUtf8Bytes(intent.modelSelectionJson) === input.permit.modelSelectionJson;
    if (!exact) {
      return yield* fail("pre-effect-validate", "authority-divergent", input.permit.admissionId);
    }
    const stageRows = yield* sql<{ readonly count: number }>`
      SELECT count(*) AS count
      FROM main.agent_control_stage_run_states stage
      JOIN main.agent_control_stage_run_lease_states lease
        ON lease.lease_id=${input.permit.stageLeaseId}
      WHERE stage.stage_run_id=${input.permit.stageRunId}
        AND typeof(stage.stage_run_id)='text'
        AND stage.project_id=${input.permit.projectId} AND typeof(stage.project_id)='text'
        AND stage.task_id=${input.permit.taskId} AND typeof(stage.task_id)='text'
        AND stage.attempt_id=${input.permit.attemptId} AND typeof(stage.attempt_id)='text'
        AND stage.stage_kind=${input.permit.stage === "initial-planning" ? "planning" : input.permit.stage}
        AND typeof(stage.stage_kind)='text' AND stage.status IN ('running','waiting')
        AND typeof(stage.status)='text'
        AND lease.project_id=stage.project_id AND lease.task_id=stage.task_id
        AND lease.stage_run_id=stage.stage_run_id AND lease.attempt_id=stage.attempt_id
        AND lease.holder_id=${input.permit.stageLeaseHolderId} AND typeof(lease.holder_id)='text'
        AND lease.fence_token=${input.permit.stageFenceToken} AND typeof(lease.fence_token)='integer'
        AND lease.status='reserved' AND typeof(lease.status)='text'
        AND lease.expires_at>${input.enteredAt} AND typeof(lease.expires_at)='text'
    `;
    if (stageRows[0]?.count !== 1) {
      return yield* fail("pre-effect-stage", "stale-owner", input.permit.admissionId);
    }
    const deliveryRows =
      input.permit.stage === "initial-planning"
        ? yield* sql<{ readonly count: number }>`
            SELECT count(*) AS count FROM main.agent_control_initial_planning_deliveries
            WHERE provider_delivery_id=${input.permit.providerDeliveryId}
              AND typeof(provider_delivery_id)='text'
              AND handoff_id=${input.permit.handoffId} AND typeof(handoff_id)='text'
              AND thread_id=${input.permit.threadId} AND typeof(thread_id)='text'
              AND provider_instance_id=${String(input.permit.providerInstanceId)}
              AND typeof(provider_instance_id)='text'
              AND state='claimed' AND typeof(state)='text'
          `
        : input.permit.stage === "implementation"
          ? yield* sql<{ readonly count: number }>`
              SELECT count(*) AS count FROM main.agent_control_implementation_deliveries
              WHERE provider_delivery_id=${input.permit.providerDeliveryId}
                AND typeof(provider_delivery_id)='text'
                AND handoff_id=${input.permit.handoffId} AND typeof(handoff_id)='text'
                AND thread_id=${input.permit.threadId} AND typeof(thread_id)='text'
                AND stage_run_id=${input.permit.stageRunId} AND attempt_id=${input.permit.attemptId}
                AND lease_id=${input.permit.stageLeaseId}
                AND lease_holder_id=${input.permit.stageLeaseHolderId}
                AND fence_token=${input.permit.stageFenceToken} AND typeof(fence_token)='integer'
                AND provider_instance_id=${String(input.permit.providerInstanceId)}
                AND model_selection_fingerprint=${input.permit.modelSelectionFingerprint}
                AND state='claimed' AND typeof(state)='text'
            `
          : yield* sql<{ readonly count: number }>`
              SELECT count(*) AS count FROM main.agent_control_verification_deliveries
              WHERE provider_delivery_id=${input.permit.providerDeliveryId}
                AND typeof(provider_delivery_id)='text'
                AND handoff_id=${input.permit.handoffId} AND typeof(handoff_id)='text'
                AND thread_id=${input.permit.threadId} AND typeof(thread_id)='text'
                AND stage_run_id=${input.permit.stageRunId} AND attempt_id=${input.permit.attemptId}
                AND lease_id=${input.permit.stageLeaseId}
                AND lease_holder_id=${input.permit.stageLeaseHolderId}
                AND fence_token=${input.permit.stageFenceToken} AND typeof(fence_token)='integer'
                AND provider_instance_id=${String(input.permit.providerInstanceId)}
                AND model_selection_fingerprint=${input.permit.modelSelectionFingerprint}
                AND state='claimed' AND typeof(state)='text'
            `;
    if (deliveryRows[0]?.count !== 1) {
      return yield* fail("pre-effect-delivery", "authority-divergent", input.permit.admissionId);
    }
    const kind = input.boundary === "session-start" ? "session-entry" : "turn-entry";
    const existingBoundaries = (yield* readCompleteAuthorityChains(
      input.permit.admissionId,
    )).filter(
      (chain) =>
        chain.authorityKind === kind &&
        chain.providerFenceToken === input.permit.providerFenceToken,
    );
    if (existingBoundaries.length !== 0) {
      if (
        existingBoundaries.length !== 1 ||
        !authorityChainMatches(existingBoundaries[0]!, {
          admissionId: input.permit.admissionId,
          authorityKind: kind,
          providerInstanceId: String(input.permit.providerInstanceId),
          ownerId: input.permit.admissionOwnerId,
          providerFenceToken: input.permit.providerFenceToken,
          details: {
            handoffId: input.permit.handoffId,
            providerDeliveryId: input.permit.providerDeliveryId,
            stageFenceToken: input.permit.stageFenceToken,
          },
        })
      ) {
        return yield* fail(
          "pre-effect-boundary-replay",
          "authority-divergent",
          input.permit.admissionId,
        );
      }
      return;
    }
    yield* appendAuthority({
      admissionId: input.permit.admissionId,
      authorityKind: kind,
      providerInstanceId: String(input.permit.providerInstanceId),
      ownerId: input.permit.admissionOwnerId,
      providerFenceToken: input.permit.providerFenceToken,
      occurredAt: input.enteredAt,
      details: {
        handoffId: input.permit.handoffId,
        providerDeliveryId: input.permit.providerDeliveryId,
        stageFenceToken: input.permit.stageFenceToken,
      },
    });
    if (current.status === "admitted") {
      yield* sql`
        UPDATE main.agent_control_provider_admission_current SET status='entered',
          revision=revision+1,updated_at=${input.enteredAt}
        WHERE admission_id=${input.permit.admissionId}
      `;
      yield* sql`
        UPDATE main.agent_control_provider_capacity_current SET active_state='entered',
          revision=revision+1,updated_at=${input.enteredAt}
        WHERE provider_instance_id=${String(input.permit.providerInstanceId)}
          AND active_admission_id=${input.permit.admissionId}
      `;
    }
  });
  const validateAndEnterInTransaction: ProviderAdmissionStoreShape["validateAndEnterInTransaction"] =
    (input) =>
      validateAndEnterInTransactionRaw(input).pipe(
        Effect.mapError((cause) =>
          isProviderAdmissionError(cause)
            ? cause
            : fail("pre-effect", "persistence", input.permit.admissionId, cause),
        ),
      );

  const quarantine: ProviderAdmissionStoreShape["quarantine"] = Effect.fn(
    "ProviderAdmissionStore.quarantine",
  )(function* (input) {
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const current = yield* readCurrent(input.permit.admissionId);
          if (current?.status === "quarantined") return;
          if (
            current?.status !== "entered" ||
            current.providerFenceToken !== input.permit.providerFenceToken ||
            current.ownerId !== input.permit.admissionOwnerId
          ) {
            return yield* fail("quarantine", "authority-divergent", input.permit.admissionId);
          }
          yield* appendAuthority({
            admissionId: input.permit.admissionId,
            authorityKind: "quarantine",
            providerInstanceId: String(input.permit.providerInstanceId),
            ownerId: input.permit.admissionOwnerId,
            providerFenceToken: input.permit.providerFenceToken,
            occurredAt: input.observedAt,
            details: { reason: input.reason },
          });
          yield* sql`
          UPDATE main.agent_control_provider_admission_current SET status='quarantined',
            revision=revision+1,updated_at=${input.observedAt}
          WHERE admission_id=${input.permit.admissionId}
        `;
          yield* sql`
          UPDATE main.agent_control_provider_capacity_current SET active_state='quarantined',
            revision=revision+1,updated_at=${input.observedAt}
          WHERE provider_instance_id=${String(input.permit.providerInstanceId)}
            AND active_admission_id=${input.permit.admissionId}
        `;
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isProviderAdmissionError(cause)
            ? cause
            : fail("quarantine", "persistence", input.permit.admissionId, cause),
        ),
      );
  });

  const quarantineIfEntered: ProviderAdmissionStoreShape["quarantineIfEntered"] = Effect.fn(
    "ProviderAdmissionStore.quarantineIfEntered",
  )(function* (input) {
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const current = yield* readCurrent(input.permit.admissionId);
          if (current === undefined) {
            return yield* fail(
              "quarantine-if-entered",
              "authority-missing",
              input.permit.admissionId,
            );
          }
          const matchesPermit =
            current.providerInstanceId === String(input.permit.providerInstanceId) &&
            current.ownerId === input.permit.admissionOwnerId &&
            current.leaseExpiresAt === input.permit.admissionLeaseExpiresAt &&
            current.providerFenceToken === input.permit.providerFenceToken &&
            current.admissionMarkerId === input.permit.admissionMarkerId &&
            current.admissionMarkerFingerprint === input.permit.admissionMarkerFingerprint;
          if (!matchesPermit) {
            return yield* fail(
              "quarantine-if-entered",
              "authority-divergent",
              input.permit.admissionId,
            );
          }
          if (
            current.status === "admitted" ||
            current.status === "quarantined" ||
            current.status === "released" ||
            current.status === "superseded"
          ) {
            return;
          }
          if (current.status !== "entered") {
            return yield* fail(
              "quarantine-if-entered",
              "authority-divergent",
              input.permit.admissionId,
            );
          }
          yield* appendAuthority({
            admissionId: input.permit.admissionId,
            authorityKind: "quarantine",
            providerInstanceId: String(input.permit.providerInstanceId),
            ownerId: input.permit.admissionOwnerId,
            providerFenceToken: input.permit.providerFenceToken,
            occurredAt: input.observedAt,
            details: { reason: "external-outcome-unknown" },
          });
          yield* sql`
            UPDATE main.agent_control_provider_admission_current SET status='quarantined',
              revision=revision+1,updated_at=${input.observedAt}
            WHERE admission_id=${input.permit.admissionId}
          `;
          yield* sql`
            UPDATE main.agent_control_provider_capacity_current SET active_state='quarantined',
              revision=revision+1,updated_at=${input.observedAt}
            WHERE provider_instance_id=${String(input.permit.providerInstanceId)}
              AND active_admission_id=${input.permit.admissionId}
          `;
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isProviderAdmissionError(cause)
            ? cause
            : fail("quarantine-if-entered", "persistence", input.permit.admissionId, cause),
        ),
      );
  });

  const recordUsage: ProviderAdmissionStoreShape["recordUsage"] = Effect.fn(
    "ProviderAdmissionStore.recordUsage",
  )(function* (providerInstanceId, evidence) {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const waiting = yield* sql<{
            readonly admissionId: string;
            readonly observedAt: string;
            readonly usageEvidenceFingerprint: string;
          }>`
          SELECT current.admission_id AS "admissionId",usage.observed_at AS "observedAt",
            current.usage_evidence_fingerprint AS "usageEvidenceFingerprint"
          FROM main.agent_control_provider_admission_current current
          JOIN main.agent_control_provider_usage_evidence usage
            ON usage.evidence_id=current.usage_evidence_id
            AND usage.admission_id=current.admission_id
          WHERE current.provider_instance_id=${providerInstanceId} AND current.status='waiting'
            AND typeof(current.provider_instance_id)='text' AND typeof(current.status)='text'
            AND typeof(usage.observed_at)='text'
            AND typeof(current.usage_evidence_fingerprint)='text'
          ORDER BY current.requested_at, current.admission_id
        `;
          for (const row of waiting) {
            const document = canonicalJson(
              usageDocument(row.admissionId, providerInstanceId, evidence),
            );
            const boundUsageFingerprint = sha256Utf8(document);
            if (evidence.observedAt < row.observedAt) continue;
            if (evidence.observedAt === row.observedAt) {
              if (boundUsageFingerprint === row.usageEvidenceFingerprint) continue;
              return yield* fail("record-usage-order", "authority-divergent", row.admissionId);
            }
            const evidenceId = usageEvidenceId(row.admissionId, boundUsageFingerprint);
            yield* sql`
            INSERT INTO main.agent_control_provider_usage_evidence (
              evidence_id,admission_id,provider_instance_id,status,source,observed_at,
              next_relevant_at,evidence_json,evidence_fingerprint
            ) VALUES (${evidenceId},${row.admissionId},${providerInstanceId},${evidence.status},
              ${evidence.source},${evidence.observedAt},${evidence.nextRelevantAt},
              ${utf8(document)},${boundUsageFingerprint})
            ON CONFLICT(admission_id,evidence_fingerprint) DO NOTHING
          `;
            yield* sql`
            UPDATE main.agent_control_provider_admission_current SET usage_status=${evidence.status},
              usage_eligible=${usageAllowsAdmission(evidence.status) ? 1 : 0},
              usage_evidence_id=${evidenceId},usage_evidence_fingerprint=${boundUsageFingerprint},
              next_deadline_at=${evidence.nextRelevantAt},revision=revision+1,
              updated_at=${evidence.observedAt}
            WHERE admission_id=${row.admissionId}
          `;
          }
          return yield* sql<ProviderAdmissionWakeup>`
          SELECT stage,handoff_id AS "handoffId",provider_instance_id AS "providerInstanceId"
          FROM main.agent_control_provider_admission_current
          INDEXED BY idx_agent_control_provider_admission_queue
          WHERE provider_instance_id=${providerInstanceId} AND status='waiting' AND usage_eligible=1
            AND typeof(provider_instance_id)='text' AND typeof(status)='text'
            AND typeof(usage_eligible)='integer' AND typeof(requested_at)='text'
            AND typeof(admission_id)='text'
          ORDER BY requested_at,admission_id
        `;
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isProviderAdmissionError(cause)
            ? cause
            : fail("record-usage", "persistence", undefined, cause),
        ),
      );
  });

  const listWaiting = sql<ProviderAdmissionWakeup>`
    SELECT stage,handoff_id AS "handoffId",provider_instance_id AS "providerInstanceId"
    FROM main.agent_control_provider_admission_current
    WHERE status='waiting' AND typeof(status)='text'
    ORDER BY provider_instance_id,requested_at,admission_id
  `.pipe(Effect.mapError((cause) => fail("list-waiting", "persistence", undefined, cause)));

  const listDueDeadlines: ProviderAdmissionStoreShape["listDueDeadlines"] = (now) =>
    Effect.all([
      sql.unsafe<ProviderAdmissionDeadlineWakeup>(PROVIDER_ADMISSION_DUE_WAITING_DEADLINES_SQL, [
        now,
      ]),
      sql.unsafe<ProviderAdmissionDeadlineWakeup>(PROVIDER_ADMISSION_DUE_ADMITTED_DEADLINES_SQL, [
        now,
      ]),
    ]).pipe(
      Effect.map(([waiting, admitted]) =>
        [...waiting, ...admitted].sort(
          (left, right) =>
            left.deadlineAt.localeCompare(right.deadlineAt) ||
            left.providerInstanceId.localeCompare(right.providerInstanceId) ||
            left.admissionId.localeCompare(right.admissionId),
        ),
      ),
      Effect.mapError((cause) => fail("list-due-deadlines", "persistence", undefined, cause)),
    );

  const listEnteredWithoutRelease = sql<CurrentRow & IntentRow>`
    SELECT current.admission_id AS "admissionId",current.provider_instance_id AS "providerInstanceId",
      current.stage,current.handoff_id AS "handoffId",current.status,
      current.requested_at AS "requestedAt",current.usage_status AS "usageStatus",
      current.usage_evidence_fingerprint AS "usageEvidenceFingerprint",
      current.owner_id AS "ownerId",current.lease_expires_at AS "leaseExpiresAt",
      current.provider_fence_token AS "providerFenceToken",
      current.admission_marker_id AS "admissionMarkerId",
      current.admission_marker_fingerprint AS "admissionMarkerFingerprint",
      intent.project_id AS "projectId",intent.task_id AS "taskId",
      intent.stage_run_id AS "stageRunId",intent.attempt_id AS "attemptId",
      intent.provider_delivery_id AS "providerDeliveryId",intent.thread_id AS "threadId",
      intent.stage_lease_id AS "stageLeaseId",intent.stage_lease_holder_id AS "stageLeaseHolderId",
      intent.stage_fence_token AS "stageFenceToken",intent.model_selection_json AS "modelSelectionJson",
      intent.model_selection_fingerprint AS "modelSelectionFingerprint",
      intent.intent_fingerprint AS "intentFingerprint"
    FROM main.agent_control_provider_admission_current current
    JOIN main.agent_control_provider_admission_intents intent ON intent.admission_id=current.admission_id
    WHERE current.status='entered' AND typeof(current.status)='text'
    ORDER BY current.provider_instance_id,current.admission_id
  `.pipe(
    Effect.flatMap((rows) =>
      Effect.forEach(rows, (row) => {
        const synthetic: CurrentRow = row;
        const permit = permitFromRows({ ...synthetic, status: "admitted" }, row);
        return permit === undefined
          ? Effect.fail(fail("list-entered", "authority-divergent", row.admissionId))
          : Effect.succeed(permit);
      }),
    ),
    Effect.mapError((cause) =>
      isProviderAdmissionError(cause)
        ? cause
        : fail("list-entered", "persistence", undefined, cause),
    ),
  );

  const minimumDeadline = Effect.all([
    sql.unsafe<{ readonly deadline: string }>(PROVIDER_ADMISSION_MINIMUM_WAITING_DEADLINE_SQL, []),
    sql.unsafe<{ readonly deadline: string }>(PROVIDER_ADMISSION_MINIMUM_ADMITTED_DEADLINE_SQL, []),
  ]).pipe(
    Effect.map(([waiting, admitted]) => {
      const deadlines = [waiting[0]?.deadline, admitted[0]?.deadline].filter(
        (deadline): deadline is string => deadline !== undefined,
      );
      return deadlines.length === 0 ? null : deadlines.sort()[0]!;
    }),
    Effect.mapError((cause) => fail("minimum-deadline", "persistence", undefined, cause)),
  );

  const minimumDeadlineAfter: ProviderAdmissionStoreShape["minimumDeadlineAfter"] = (after) =>
    Effect.all([
      sql.unsafe<{ readonly deadline: string }>(
        PROVIDER_ADMISSION_MINIMUM_WAITING_DEADLINE_AFTER_SQL,
        [after],
      ),
      sql.unsafe<{ readonly deadline: string }>(
        PROVIDER_ADMISSION_MINIMUM_ADMITTED_DEADLINE_AFTER_SQL,
        [after],
      ),
    ]).pipe(
      Effect.map(([waiting, admitted]) => {
        const deadlines = [waiting[0]?.deadline, admitted[0]?.deadline].filter(
          (deadline): deadline is string => deadline !== undefined,
        );
        return deadlines.length === 0 ? null : deadlines.sort()[0]!;
      }),
      Effect.mapError((cause) => fail("minimum-deadline-after", "persistence", undefined, cause)),
    );

  const loadFinalization = Effect.fn("ProviderAdmissionStore.loadFinalization")(function* (
    stage: ProviderAdmissionStage,
    handoffId: string,
  ) {
    const rows =
      stage === "initial-planning"
        ? yield* sql<FinalizationRow>`
            SELECT evidence.handoff_id AS "handoffId",evidence.project_id AS "projectId",
              evidence.task_id AS "taskId",evidence.stage_run_id AS "stageRunId",
              evidence.attempt_id AS "attemptId",evidence.lease_id AS "leaseId",
              evidence.lease_holder_id AS "leaseHolderId",evidence.fence_token AS "fenceToken",
              evidence.provider_delivery_id AS "providerDeliveryId",
              evidence.provider_instance_id AS "providerInstanceId",
              NULL AS "terminalRuntimeEventId",
              terminal.event_id AS "terminalOrchestrationEventId",
              terminal.event_type AS "terminalEventType",
              terminal.stream_version AS "terminalStreamVersion",
              typeof(terminal.metadata_json) AS "terminalMetadataStorageClass",
              CAST(terminal.metadata_json AS BLOB) AS "terminalMetadataBytes",
              terminal.metadata_json AS "terminalMetadataText",
              marker.marker_id AS "finalizationMarkerId",
              marker.marker_fingerprint AS "finalizationMarkerFingerprint",
              evidence.finalized_at AS "finalizedAt"
            FROM main.agent_control_initial_planning_result_evidence evidence
            JOIN main.agent_control_initial_planning_finalization_receipts receipt
              ON receipt.result_evidence_id=evidence.result_evidence_id
              AND receipt.finalization_command_id=evidence.finalization_command_id
              AND receipt.finalization_fingerprint=evidence.finalization_fingerprint
              AND receipt.handoff_id=evidence.handoff_id
              AND receipt.accepted_at=evidence.finalized_at
            JOIN main.agent_control_initial_planning_finalization_markers marker
              ON marker.finalization_command_id=receipt.finalization_command_id
              AND marker.result_evidence_id=evidence.result_evidence_id
              AND marker.handoff_id=evidence.handoff_id
              AND marker.committed_at=receipt.accepted_at
            JOIN main.orchestration_events terminal
              ON terminal.event_id=evidence.orchestration_terminal_event_id
            WHERE evidence.handoff_id=${handoffId}
              AND typeof(evidence.handoff_id)='text' AND typeof(evidence.project_id)='text'
              AND typeof(evidence.task_id)='text' AND typeof(evidence.stage_run_id)='text'
              AND typeof(evidence.attempt_id)='text' AND typeof(evidence.lease_id)='text'
              AND typeof(evidence.lease_holder_id)='text' AND typeof(evidence.fence_token)='integer'
              AND typeof(evidence.provider_delivery_id)='text'
              AND typeof(evidence.provider_instance_id)='text'
              AND typeof(marker.marker_id)='text' AND typeof(marker.marker_fingerprint)='text'
              AND typeof(evidence.finalized_at)='text' AND typeof(terminal.event_id)='text'
              AND typeof(terminal.event_type)='text' AND typeof(terminal.stream_version)='integer'
          `
        : stage === "implementation"
          ? yield* sql<FinalizationRow>`
              SELECT evidence.handoff_id AS "handoffId",evidence.project_id AS "projectId",
                evidence.task_id AS "taskId",evidence.stage_run_id AS "stageRunId",
                evidence.attempt_id AS "attemptId",evidence.lease_id AS "leaseId",
                evidence.lease_holder_id AS "leaseHolderId",evidence.fence_token AS "fenceToken",
                evidence.provider_delivery_id AS "providerDeliveryId",
                evidence.provider_instance_id AS "providerInstanceId",
                NULL AS "terminalRuntimeEventId",
                terminal.event_id AS "terminalOrchestrationEventId",
                terminal.event_type AS "terminalEventType",
                terminal.stream_version AS "terminalStreamVersion",
                typeof(terminal.metadata_json) AS "terminalMetadataStorageClass",
                CAST(terminal.metadata_json AS BLOB) AS "terminalMetadataBytes",
                terminal.metadata_json AS "terminalMetadataText",
                marker.marker_id AS "finalizationMarkerId",
                marker.marker_fingerprint AS "finalizationMarkerFingerprint",
                evidence.finalized_at AS "finalizedAt"
              FROM main.agent_control_implementation_result_evidence evidence
              JOIN main.agent_control_implementation_stage_finalization_receipts receipt
                ON receipt.receipt_id=evidence.receipt_id AND receipt.marker_id=evidence.marker_id
                AND receipt.result_evidence_id=evidence.result_evidence_id
                AND receipt.finalization_command_id=evidence.finalization_command_id
                AND receipt.finalization_fingerprint=evidence.finalization_fingerprint
                AND receipt.handoff_id=evidence.handoff_id AND receipt.status='accepted'
                AND receipt.accepted_at=evidence.finalized_at
              JOIN main.agent_control_implementation_stage_finalization_markers marker
                ON marker.marker_id=evidence.marker_id AND marker.receipt_id=evidence.receipt_id
                AND marker.result_evidence_id=evidence.result_evidence_id
                AND marker.finalization_command_id=evidence.finalization_command_id
                AND marker.finalization_fingerprint=evidence.finalization_fingerprint
                AND marker.handoff_id=evidence.handoff_id AND marker.committed_at=receipt.accepted_at
              JOIN main.orchestration_events terminal
                ON terminal.event_id=evidence.orchestration_terminal_event_id
              WHERE evidence.handoff_id=${handoffId}
                AND typeof(evidence.handoff_id)='text' AND typeof(evidence.project_id)='text'
                AND typeof(evidence.task_id)='text' AND typeof(evidence.stage_run_id)='text'
                AND typeof(evidence.attempt_id)='text' AND typeof(evidence.lease_id)='text'
                AND typeof(evidence.lease_holder_id)='text' AND typeof(evidence.fence_token)='integer'
                AND typeof(evidence.provider_delivery_id)='text'
                AND typeof(evidence.provider_instance_id)='text'
                AND typeof(marker.marker_id)='text' AND typeof(marker.marker_fingerprint)='text'
                AND typeof(evidence.finalized_at)='text' AND typeof(terminal.event_id)='text'
                AND typeof(terminal.event_type)='text' AND typeof(terminal.stream_version)='integer'
            `
          : yield* sql<FinalizationRow>`
              SELECT evidence.handoff_id AS "handoffId",evidence.project_id AS "projectId",
                evidence.task_id AS "taskId",evidence.stage_run_id AS "stageRunId",
                evidence.attempt_id AS "attemptId",evidence.lease_id AS "leaseId",
                evidence.lease_holder_id AS "leaseHolderId",evidence.fence_token AS "fenceToken",
                evidence.provider_delivery_id AS "providerDeliveryId",
                evidence.provider_instance_id AS "providerInstanceId",
                evidence.terminal_runtime_event_id AS "terminalRuntimeEventId",
                terminal.event_id AS "terminalOrchestrationEventId",
                terminal.event_type AS "terminalEventType",
                terminal.stream_version AS "terminalStreamVersion",
                typeof(terminal.metadata_json) AS "terminalMetadataStorageClass",
                CAST(terminal.metadata_json AS BLOB) AS "terminalMetadataBytes",
                terminal.metadata_json AS "terminalMetadataText",
                marker.marker_id AS "finalizationMarkerId",
                marker.marker_fingerprint AS "finalizationMarkerFingerprint",
                evidence.finalized_at AS "finalizedAt"
              FROM main.agent_control_verification_finalization_evidence evidence
              JOIN main.agent_control_verification_finalization_receipts receipt
                ON receipt.receipt_id=evidence.receipt_id AND receipt.marker_id=evidence.marker_id
                AND receipt.finalization_evidence_id=evidence.finalization_evidence_id
                AND receipt.finalization_command_id=evidence.finalization_command_id
                AND receipt.finalization_fingerprint=evidence.finalization_fingerprint
                AND receipt.handoff_id=evidence.handoff_id AND receipt.status='accepted'
                AND receipt.accepted_at=evidence.finalized_at
              JOIN main.agent_control_verification_finalization_markers marker
                ON marker.marker_id=evidence.marker_id AND marker.receipt_id=evidence.receipt_id
                AND marker.finalization_evidence_id=evidence.finalization_evidence_id
                AND marker.finalization_command_id=evidence.finalization_command_id
                AND marker.finalization_fingerprint=evidence.finalization_fingerprint
                AND marker.handoff_id=evidence.handoff_id AND marker.committed_at=receipt.accepted_at
              JOIN main.orchestration_events terminal
                ON terminal.event_id=evidence.terminal_runtime_event_id
              WHERE evidence.handoff_id=${handoffId}
                AND typeof(evidence.handoff_id)='text' AND typeof(evidence.project_id)='text'
                AND typeof(evidence.task_id)='text' AND typeof(evidence.stage_run_id)='text'
                AND typeof(evidence.attempt_id)='text' AND typeof(evidence.lease_id)='text'
                AND typeof(evidence.lease_holder_id)='text' AND typeof(evidence.fence_token)='integer'
                AND typeof(evidence.provider_delivery_id)='text'
                AND typeof(evidence.provider_instance_id)='text'
                AND typeof(evidence.terminal_runtime_event_id)='text'
                AND typeof(marker.marker_id)='text' AND typeof(marker.marker_fingerprint)='text'
                AND typeof(evidence.finalized_at)='text' AND typeof(terminal.event_id)='text'
                AND typeof(terminal.event_type)='text' AND typeof(terminal.stream_version)='integer'
            `;
    if (rows.length !== 1) return undefined;
    return rows[0]!;
  });

  const releaseFromFinalizationInTransactionRaw = Effect.fn(
    "ProviderAdmissionStore.releaseFromFinalizationInTransaction",
  )(function* (input: {
    readonly stage: ProviderAdmissionStage;
    readonly handoffId: string;
    readonly finalizedAt: string;
  }) {
    const finalization = yield* loadFinalization(input.stage, input.handoffId);
    if (finalization === undefined) return null;
    const currentRows = yield* sql<CurrentRow & IntentRow>`
        SELECT current.admission_id AS "admissionId",
          current.provider_instance_id AS "providerInstanceId",current.stage,
          current.handoff_id AS "handoffId",current.status,current.requested_at AS "requestedAt",
          current.usage_status AS "usageStatus",
          current.usage_evidence_fingerprint AS "usageEvidenceFingerprint",
          current.owner_id AS "ownerId",current.lease_expires_at AS "leaseExpiresAt",
          current.provider_fence_token AS "providerFenceToken",
          current.admission_marker_id AS "admissionMarkerId",
          current.admission_marker_fingerprint AS "admissionMarkerFingerprint",
          intent.project_id AS "projectId",intent.task_id AS "taskId",
          intent.stage_run_id AS "stageRunId",intent.attempt_id AS "attemptId",
          intent.provider_delivery_id AS "providerDeliveryId",intent.thread_id AS "threadId",
          intent.stage_lease_id AS "stageLeaseId",
          intent.stage_lease_holder_id AS "stageLeaseHolderId",
          intent.stage_fence_token AS "stageFenceToken",
          intent.model_selection_json AS "modelSelectionJson",
          intent.model_selection_fingerprint AS "modelSelectionFingerprint",
          intent.intent_fingerprint AS "intentFingerprint"
        FROM main.agent_control_provider_admission_current current
        JOIN main.agent_control_provider_admission_intents intent
          ON intent.admission_id=current.admission_id
        WHERE current.handoff_id=${input.handoffId} AND current.stage=${input.stage}
          AND typeof(current.handoff_id)='text' AND typeof(current.stage)='text'
      `;
    // Human/manual turns and durable work committed before migration 065 have
    // no ProviderAdmission identity. Their existing finalization authority
    // remains complete and must not be made dependent on this v1 slice.
    if (currentRows.length === 0) return null;
    if (currentRows.length !== 1) {
      return yield* fail("release-admission", "authority-missing");
    }
    const current = currentRows[0]!;
    if (
      finalization.finalizedAt !== input.finalizedAt ||
      finalization.handoffId !== current.handoffId ||
      finalization.projectId !== current.projectId ||
      finalization.taskId !== current.taskId ||
      finalization.stageRunId !== current.stageRunId ||
      finalization.attemptId !== current.attemptId ||
      finalization.leaseId !== current.stageLeaseId ||
      finalization.leaseHolderId !== current.stageLeaseHolderId ||
      finalization.fenceToken !== current.stageFenceToken ||
      finalization.providerDeliveryId !== current.providerDeliveryId ||
      finalization.providerInstanceId !== current.providerInstanceId
    ) {
      return yield* fail("release-binding", "authority-divergent", current.admissionId);
    }
    const metadata = yield* Effect.try({
      try: () =>
        decodePersistedOrchestrationMetadata({
          storageClass: finalization.terminalMetadataStorageClass,
          bytes: finalization.terminalMetadataBytes,
          text: finalization.terminalMetadataText,
        }),
      catch: (cause) =>
        fail("release-runtime-metadata", "authority-divergent", current.admissionId, cause),
    });
    const correlation = metadata.value.providerRuntimeMessage;
    if (
      (correlation === undefined && finalization.terminalRuntimeEventId !== null) ||
      (correlation !== undefined &&
        (correlation.providerInstanceId !== current.providerInstanceId ||
          correlation.runtimeEventId !==
            (finalization.terminalRuntimeEventId ?? correlation.runtimeEventId)))
    ) {
      return yield* fail("release-runtime-binding", "authority-divergent", current.admissionId);
    }
    const finalizationDetails = {
      attemptId: current.attemptId,
      finalizationMarkerFingerprint: finalization.finalizationMarkerFingerprint,
      finalizationMarkerId: finalization.finalizationMarkerId,
      handoffId: current.handoffId,
      projectId: current.projectId,
      providerDeliveryId: current.providerDeliveryId,
      stageFenceToken: current.stageFenceToken,
      stageRunId: current.stageRunId,
      taskId: current.taskId,
      terminalOrchestrationEventId: finalization.terminalOrchestrationEventId,
      terminalEventType: finalization.terminalEventType,
      providerRuntimeEventType: correlation?.eventType ?? null,
      terminalRuntimeEventId: correlation?.runtimeEventId ?? null,
      terminalStreamVersion: finalization.terminalStreamVersion,
    } as const;
    if (
      current.status === "waiting" ||
      current.status === "admitted" ||
      current.status === "superseded"
    ) {
      if (current.status === "admitted") {
        const entryMarkers = yield* sql<{ readonly count: number }>`
          SELECT count(*) AS count
          FROM main.agent_control_provider_authority_markers
          WHERE admission_id=${current.admissionId}
            AND authority_kind IN ('session-entry','turn-entry')
        `;
        if (
          current.ownerId === null ||
          current.providerFenceToken === null ||
          entryMarkers[0]?.count !== 0
        ) {
          return yield* fail("supersede-entry", "authority-divergent", current.admissionId);
        }
      }
      const supersedeOwnerId =
        current.ownerId ?? `provider-finalization:${finalization.finalizationMarkerId}`;
      const supersedeFence = current.providerFenceToken ?? 0;
      yield* appendAuthority({
        admissionId: current.admissionId,
        authorityKind: "supersede",
        providerInstanceId: current.providerInstanceId,
        ownerId: supersedeOwnerId,
        providerFenceToken: supersedeFence,
        occurredAt: finalization.finalizedAt,
        ...(correlation === undefined
          ? {}
          : { terminalRuntimeEventId: correlation.runtimeEventId }),
        terminalEventType: finalization.terminalEventType,
        terminalStreamVersion: finalization.terminalStreamVersion,
        finalizationMarkerId: finalization.finalizationMarkerId,
        finalizationMarkerFingerprint: finalization.finalizationMarkerFingerprint,
        details: finalizationDetails,
      });
      if (current.status !== "superseded") {
        yield* sql`
          UPDATE main.agent_control_provider_admission_current SET status='superseded',
            revision=revision+1,updated_at=MAX(updated_at,${finalization.finalizedAt})
          WHERE admission_id=${current.admissionId}
        `;
        if (current.status === "admitted") {
          yield* sql`
            UPDATE main.agent_control_provider_capacity_current SET
              active_admission_id=NULL,active_state=NULL,active_owner_id=NULL,
              active_lease_expires_at=NULL,active_fence_token=NULL,
              active_marker_fingerprint=NULL,revision=revision+1,
              updated_at=MAX(updated_at,${finalization.finalizedAt})
            WHERE provider_instance_id=${current.providerInstanceId}
              AND active_admission_id=${current.admissionId}
              AND active_fence_token=${current.providerFenceToken}
          `;
        }
      }
      return current.providerInstanceId;
    }
    if (
      current.status !== "entered" &&
      current.status !== "quarantined" &&
      current.status !== "released"
    ) {
      return yield* fail("release-state", "authority-divergent", current.admissionId);
    }
    if (current.ownerId === null || current.providerFenceToken === null) {
      return yield* fail("release-owner", "authority-divergent", current.admissionId);
    }
    if (correlation === undefined) {
      return yield* fail("release-runtime-binding", "authority-divergent", current.admissionId);
    }
    yield* appendAuthority({
      admissionId: current.admissionId,
      authorityKind: "release",
      providerInstanceId: current.providerInstanceId,
      ownerId: current.ownerId,
      providerFenceToken: current.providerFenceToken,
      occurredAt: finalization.finalizedAt,
      terminalRuntimeEventId: correlation.runtimeEventId,
      terminalEventType: finalization.terminalEventType,
      terminalStreamVersion: finalization.terminalStreamVersion,
      finalizationMarkerId: finalization.finalizationMarkerId,
      finalizationMarkerFingerprint: finalization.finalizationMarkerFingerprint,
      details: finalizationDetails,
    });
    if (current.status !== "released") {
      yield* sql`
          UPDATE main.agent_control_provider_admission_current SET status='released',
            revision=revision+1,updated_at=MAX(updated_at,${finalization.finalizedAt})
          WHERE admission_id=${current.admissionId}
        `;
      yield* sql`
          UPDATE main.agent_control_provider_capacity_current SET
            active_admission_id=NULL,active_state=NULL,active_owner_id=NULL,
            active_lease_expires_at=NULL,active_fence_token=NULL,
            active_marker_fingerprint=NULL,revision=revision+1,
            updated_at=MAX(updated_at,${finalization.finalizedAt})
          WHERE provider_instance_id=${current.providerInstanceId}
            AND active_admission_id=${current.admissionId}
            AND active_fence_token=${current.providerFenceToken}
        `;
    }
    return current.providerInstanceId;
  });
  const releaseFromFinalizationInTransaction: ProviderAdmissionStoreShape["releaseFromFinalizationInTransaction"] =
    (input) =>
      releaseFromFinalizationInTransactionRaw(input).pipe(
        Effect.mapError((cause) =>
          isProviderAdmissionError(cause)
            ? cause
            : fail("release", "persistence", undefined, cause),
        ),
      );

  const catchUpFinalized: ProviderAdmissionStoreShape["catchUpFinalized"] = sql<{
    readonly stage: ProviderAdmissionStage;
    readonly handoffId: string;
    readonly finalizedAt: string;
  }>`
      SELECT current.stage,current.handoff_id AS "handoffId",
        CASE current.stage
          WHEN 'initial-planning' THEN initial.finalized_at
          WHEN 'implementation' THEN implementation.finalized_at
          ELSE verification.finalized_at
        END AS "finalizedAt"
      FROM main.agent_control_provider_admission_current current
      LEFT JOIN main.agent_control_initial_planning_result_evidence initial
        ON current.stage='initial-planning' AND initial.handoff_id=current.handoff_id
      LEFT JOIN main.agent_control_implementation_result_evidence implementation
        ON current.stage='implementation' AND implementation.handoff_id=current.handoff_id
      LEFT JOIN main.agent_control_verification_finalization_evidence verification
        ON current.stage='verification' AND verification.handoff_id=current.handoff_id
      WHERE current.status IN ('waiting','admitted','entered','quarantined','superseded')
        AND typeof(current.status)='text'
        AND CASE current.stage
          WHEN 'initial-planning' THEN initial.handoff_id IS NOT NULL
          WHEN 'implementation' THEN implementation.handoff_id IS NOT NULL
          ELSE verification.handoff_id IS NOT NULL
        END
      ORDER BY current.provider_instance_id,current.admission_id
    `.pipe(
    Effect.flatMap((rows) =>
      Effect.forEach(rows, (row) => sql.withTransaction(releaseFromFinalizationInTransaction(row))),
    ),
    Effect.map((providers) =>
      providers.filter((provider): provider is string => provider !== null),
    ),
    Effect.mapError((cause) =>
      isProviderAdmissionError(cause)
        ? cause
        : fail("release-catch-up", "persistence", undefined, cause),
    ),
  );

  const auditStartupAuthority = Effect.fn("ProviderAdmissionStore.auditStartupAuthority")(
    function* () {
      const objects = yield* sql<{ readonly name: string; readonly source: string }>`
      SELECT name,sql AS source FROM main.sqlite_schema
      WHERE name IN ${sql.in(PROVIDER_ADMISSION_SCHEMA_OBJECTS)} AND sql IS NOT NULL
      ORDER BY name
    `;
      if (
        objects.length !== PROVIDER_ADMISSION_SCHEMA_OBJECTS.length ||
        objects.some(
          (object) =>
            EXPECTED_PROVIDER_ADMISSION_DDL_FINGERPRINTS[object.name] !== sha256Utf8(object.source),
        )
      ) {
        return yield* fail("startup-ddl-audit", "authority-divergent");
      }
      const authorityCounts = yield* sql<{
        readonly evidenceCount: number;
        readonly receiptCount: number;
        readonly markerCount: number;
        readonly completeCount: number;
      }>`
        SELECT
          (SELECT count(*) FROM main.agent_control_provider_authority_evidence) AS "evidenceCount",
          (SELECT count(*) FROM main.agent_control_provider_authority_receipts) AS "receiptCount",
          (SELECT count(*) FROM main.agent_control_provider_authority_markers) AS "markerCount",
          (SELECT count(*)
           FROM main.agent_control_provider_authority_evidence evidence
           JOIN main.agent_control_provider_authority_receipts receipt
             ON receipt.receipt_id=evidence.receipt_id
             AND receipt.evidence_id=evidence.evidence_id
             AND receipt.marker_id=evidence.marker_id
             AND receipt.admission_id=evidence.admission_id
             AND receipt.authority_kind=evidence.authority_kind
             AND receipt.status='accepted'
             AND receipt.accepted_at=evidence.occurred_at
           JOIN main.agent_control_provider_authority_markers marker
             ON marker.marker_id=evidence.marker_id
             AND marker.evidence_id=evidence.evidence_id
             AND marker.receipt_id=evidence.receipt_id
             AND marker.admission_id=evidence.admission_id
             AND marker.authority_kind=evidence.authority_kind
             AND marker.marker_fingerprint=evidence.payload_fingerprint
             AND marker.committed_at=evidence.occurred_at
           WHERE typeof(evidence.evidence_id)='text'
             AND typeof(evidence.receipt_id)='text'
             AND typeof(evidence.marker_id)='text'
             AND typeof(evidence.admission_id)='text'
             AND typeof(evidence.authority_kind)='text'
             AND typeof(evidence.provider_instance_id)='text'
             AND typeof(evidence.owner_id)='text'
             AND typeof(evidence.provider_fence_token)='integer'
             AND typeof(evidence.occurred_at)='text'
             AND typeof(evidence.payload_json)='blob'
             AND typeof(evidence.payload_fingerprint)='text'
             AND typeof(receipt.receipt_id)='text'
             AND typeof(receipt.status)='text'
             AND typeof(receipt.accepted_at)='text'
             AND typeof(marker.marker_id)='text'
             AND typeof(marker.marker_fingerprint)='text'
             AND typeof(marker.committed_at)='text') AS "completeCount"
      `;
      const counts = authorityCounts[0];
      if (
        counts === undefined ||
        counts.evidenceCount !== counts.completeCount ||
        counts.receiptCount !== counts.completeCount ||
        counts.markerCount !== counts.completeCount
      ) {
        return yield* fail("startup-authority-chain-audit", "authority-divergent");
      }
      const admissionProblems = yield* sql<{ readonly count: number }>`
      SELECT count(*) AS count
      FROM main.agent_control_provider_admission_intents intent
      LEFT JOIN main.agent_control_provider_admission_current current
        ON current.admission_id=intent.admission_id
      LEFT JOIN main.agent_control_provider_usage_evidence usage
        ON usage.evidence_id=current.usage_evidence_id
      WHERE current.admission_id IS NULL
        OR intent.provider_instance_id!=current.provider_instance_id
        OR intent.stage!=current.stage OR intent.handoff_id!=current.handoff_id
        OR intent.requested_at!=current.requested_at
        OR usage.admission_id IS NULL OR usage.admission_id!=current.admission_id
        OR usage.provider_instance_id!=current.provider_instance_id
        OR usage.status!=current.usage_status
        OR usage.evidence_fingerprint!=current.usage_evidence_fingerprint
        OR usage.next_relevant_at IS NOT current.next_deadline_at
        OR current.status='claimed'
        OR (current.status IN ('admitted','entered','quarantined','released') AND NOT EXISTS (
          SELECT 1
          FROM main.agent_control_provider_authority_markers marker
          JOIN main.agent_control_provider_authority_evidence evidence
            ON evidence.evidence_id=marker.evidence_id
          WHERE marker.marker_id=current.admission_marker_id
            AND marker.admission_id=current.admission_id
            AND marker.authority_kind='admission'
            AND marker.marker_fingerprint=current.admission_marker_fingerprint
            AND evidence.owner_id=current.owner_id
            AND evidence.provider_fence_token=current.provider_fence_token
            AND NOT EXISTS (
              SELECT 1 FROM main.agent_control_provider_authority_evidence newer
              WHERE newer.admission_id=current.admission_id
                AND newer.authority_kind='admission'
                AND newer.provider_fence_token>evidence.provider_fence_token
            )
        ))
        OR (current.status='superseded' AND NOT EXISTS (
          SELECT 1 FROM main.agent_control_provider_authority_markers marker
          WHERE marker.admission_id=current.admission_id AND marker.authority_kind='supersede'
        ))
    `;
      const capacityProblems = yield* sql<{ readonly count: number }>`
      SELECT count(*) AS count
      FROM main.agent_control_provider_capacity_current capacity
      WHERE (NOT EXISTS (
          SELECT 1 FROM main.agent_control_provider_admission_intents intent
          WHERE intent.provider_instance_id=capacity.provider_instance_id
        ) AND NOT (
          capacity.last_fence_token=0 AND capacity.revision=1
          AND capacity.active_admission_id IS NULL AND capacity.active_state IS NULL
          AND capacity.active_owner_id IS NULL AND capacity.active_lease_expires_at IS NULL
          AND capacity.active_fence_token IS NULL AND capacity.active_marker_fingerprint IS NULL
        ))
        OR capacity.last_fence_token!=COALESCE((
          SELECT MAX(history.provider_fence_token)
          FROM main.agent_control_provider_claim_history history
          WHERE history.provider_instance_id=capacity.provider_instance_id
        ),0)
        OR (capacity.active_admission_id IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM main.agent_control_provider_admission_current admission
          WHERE admission.admission_id=capacity.active_admission_id
            AND admission.provider_instance_id=capacity.provider_instance_id
            AND admission.status=capacity.active_state
            AND admission.owner_id=capacity.active_owner_id
            AND admission.lease_expires_at=capacity.active_lease_expires_at
            AND admission.provider_fence_token=capacity.active_fence_token
            AND admission.admission_marker_fingerprint IS capacity.active_marker_fingerprint
        ))
        OR (capacity.active_admission_id IS NULL AND EXISTS (
          SELECT 1 FROM main.agent_control_provider_admission_current admission
          WHERE admission.provider_instance_id=capacity.provider_instance_id
            AND admission.status IN ('admitted','entered','quarantined')
        ))
    `;
      const sourceProblems = yield* sql<{
        readonly orphanCurrentCount: number;
        readonly missingCapacityCount: number;
        readonly claimProblemCount: number;
        readonly authorityProblemCount: number;
        readonly usageProblemCount: number;
      }>`
        SELECT
          (SELECT count(*)
           FROM main.agent_control_provider_admission_current current
           WHERE NOT EXISTS (
             SELECT 1 FROM main.agent_control_provider_admission_intents intent
             WHERE intent.admission_id=current.admission_id
           )) AS "orphanCurrentCount",
          (SELECT count(*)
           FROM (SELECT DISTINCT provider_instance_id
                 FROM main.agent_control_provider_admission_intents) provider
           WHERE NOT EXISTS (
             SELECT 1 FROM main.agent_control_provider_capacity_current capacity
             WHERE capacity.provider_instance_id=provider.provider_instance_id
           )) AS "missingCapacityCount",
          (SELECT count(*)
           FROM main.agent_control_provider_claim_history claim
           LEFT JOIN main.agent_control_provider_admission_intents intent
             ON intent.admission_id=claim.admission_id
           WHERE intent.admission_id IS NULL
             OR intent.provider_instance_id!=claim.provider_instance_id
             OR typeof(claim.claim_id)!='text'
             OR typeof(claim.admission_id)!='text'
             OR typeof(claim.provider_instance_id)!='text'
             OR typeof(claim.owner_id)!='text'
             OR typeof(claim.provider_fence_token)!='integer'
             OR typeof(claim.claimed_at)!='text'
             OR typeof(claim.lease_expires_at)!='text'
             OR typeof(claim.claim_fingerprint)!='text') AS "claimProblemCount",
          (SELECT count(*)
           FROM main.agent_control_provider_authority_evidence authority
           LEFT JOIN main.agent_control_provider_admission_intents intent
             ON intent.admission_id=authority.admission_id
           WHERE intent.admission_id IS NULL
             OR intent.provider_instance_id!=authority.provider_instance_id) AS "authorityProblemCount",
          (SELECT count(*)
           FROM main.agent_control_provider_usage_evidence usage
           LEFT JOIN main.agent_control_provider_admission_intents intent
             ON intent.admission_id=usage.admission_id
           WHERE intent.admission_id IS NULL
             OR intent.provider_instance_id!=usage.provider_instance_id) AS "usageProblemCount"
      `;
      const sourceProblem = sourceProblems[0];
      if (
        admissionProblems[0]?.count !== 0 ||
        capacityProblems[0]?.count !== 0 ||
        sourceProblem === undefined ||
        sourceProblem.orphanCurrentCount !== 0 ||
        sourceProblem.missingCapacityCount !== 0 ||
        sourceProblem.claimProblemCount !== 0 ||
        sourceProblem.authorityProblemCount !== 0 ||
        sourceProblem.usageProblemCount !== 0
      ) {
        return yield* fail("startup-projection-audit", "authority-divergent");
      }

      const claims = yield* sql<ClaimRow>`
        SELECT claim_id AS "claimId",admission_id AS "admissionId",
          provider_instance_id AS "providerInstanceId",owner_id AS "ownerId",
          provider_fence_token AS "providerFenceToken",claimed_at AS "claimedAt",
          lease_expires_at AS "leaseExpiresAt",claim_fingerprint AS "claimFingerprint"
        FROM main.agent_control_provider_claim_history
        ORDER BY provider_instance_id,provider_fence_token
      `;
      const providers = new Map<string, ReadonlyArray<ClaimRow>>();
      for (const claim of claims) {
        const providerClaims = providers.get(claim.providerInstanceId) ?? [];
        providers.set(claim.providerInstanceId, [...providerClaims, claim]);
      }
      for (const providerClaims of providers.values()) {
        for (const [index, claim] of providerClaims.entries()) {
          if (claim.providerFenceToken !== index + 1) {
            return yield* fail(
              "startup-provider-fence-audit",
              "authority-divergent",
              claim.admissionId,
            );
          }
        }
      }

      const intentIds = yield* sql<{ readonly admissionId: string }>`
        SELECT admission_id AS "admissionId"
        FROM main.agent_control_provider_admission_intents
        ORDER BY admission_id
      `;
      for (const { admissionId } of intentIds) {
        const [current, intent, chains] = yield* Effect.all([
          readCurrent(admissionId),
          readIntent(admissionId),
          readCompleteAuthorityChains(admissionId),
        ]);
        if (current === undefined || intent === undefined) {
          return yield* fail("startup-authority-history-audit", "authority-divergent", admissionId);
        }
        const admissionChains = chains.filter((chain) => chain.authorityKind === "admission");
        const entryChains = chains.filter(
          (chain) =>
            chain.authorityKind === "session-entry" || chain.authorityKind === "turn-entry",
        );
        const quarantineChains = chains.filter((chain) => chain.authorityKind === "quarantine");
        const supersedeChains = chains.filter((chain) => chain.authorityKind === "supersede");
        const releaseChains = chains.filter((chain) => chain.authorityKind === "release");
        const admissionClaims = claims.filter((claim) => claim.admissionId === admissionId);
        const newestMarkerSequence = chains.reduce(
          (latest, chain) => Math.max(latest, chain.markerSequence),
          0,
        );
        const isNewest = (chain: AuthorityChainRow) =>
          chain.markerSequence === newestMarkerSequence;

        for (const chain of [...admissionChains, ...entryChains]) {
          const details =
            chain.authorityKind === "admission"
              ? {
                  handoffId: intent.handoffId,
                  providerDeliveryId: intent.providerDeliveryId,
                  usageEvidenceFingerprint: current.usageEvidenceFingerprint,
                }
              : {
                  handoffId: intent.handoffId,
                  providerDeliveryId: intent.providerDeliveryId,
                  stageFenceToken: intent.stageFenceToken,
                };
          if (
            !authorityChainMatches(chain, {
              admissionId,
              authorityKind: chain.authorityKind,
              providerInstanceId: current.providerInstanceId,
              ownerId: chain.ownerId,
              providerFenceToken: chain.providerFenceToken,
              details,
            })
          ) {
            return yield* fail(
              "startup-authority-history-audit",
              "authority-divergent",
              admissionId,
            );
          }
        }
        if (admissionClaims.length !== admissionChains.length) {
          return yield* fail("startup-claim-history-audit", "authority-divergent", admissionId);
        }
        for (const claim of admissionClaims) {
          const matchingAdmissionChains = admissionChains.filter(
            (chain) =>
              chain.providerInstanceId === claim.providerInstanceId &&
              chain.ownerId === claim.ownerId &&
              chain.providerFenceToken === claim.providerFenceToken &&
              chain.occurredAt === claim.claimedAt,
          );
          if (
            claim.claimFingerprint !==
              sha256Utf8(
                canonicalJson([
                  claim.admissionId,
                  claim.ownerId,
                  claim.providerFenceToken,
                  claim.claimedAt,
                  claim.leaseExpiresAt,
                ]),
              ) ||
            matchingAdmissionChains.length !== 1
          ) {
            return yield* fail("startup-claim-history-audit", "authority-divergent", admissionId);
          }
        }
        for (const chain of chains) {
          if (chain.authorityKind === "admission") continue;
          if (chain.authorityKind === "supersede" && chain.providerFenceToken === 0) continue;
          if (
            !admissionChains.some(
              (admission) =>
                admission.providerInstanceId === chain.providerInstanceId &&
                admission.ownerId === chain.ownerId &&
                admission.providerFenceToken === chain.providerFenceToken,
            )
          ) {
            return yield* fail("startup-authority-fence-audit", "authority-divergent", admissionId);
          }
        }
        for (const chain of quarantineChains) {
          const matchesReason = (
            ["external-outcome-unknown", "owner-lost-after-entry"] as const
          ).some((reason) =>
            authorityChainMatches(chain, {
              admissionId,
              authorityKind: "quarantine",
              providerInstanceId: current.providerInstanceId,
              ownerId: chain.ownerId,
              providerFenceToken: chain.providerFenceToken,
              details: { reason },
            }),
          );
          if (!matchesReason) {
            return yield* fail(
              "startup-authority-history-audit",
              "authority-divergent",
              admissionId,
            );
          }
        }

        const currentAdmissionChain =
          current.admissionMarkerId === null || current.providerFenceToken === null
            ? undefined
            : admissionChains.filter(
                (chain) =>
                  chain.markerId === current.admissionMarkerId &&
                  chain.markerFingerprint === current.admissionMarkerFingerprint &&
                  chain.providerInstanceId === current.providerInstanceId &&
                  chain.ownerId === current.ownerId &&
                  chain.providerFenceToken === current.providerFenceToken,
              );
        if (
          (current.status === "admitted" ||
            current.status === "entered" ||
            current.status === "quarantined" ||
            current.status === "released" ||
            (current.status === "superseded" && current.admissionMarkerId !== null)) &&
          currentAdmissionChain?.length !== 1
        ) {
          return yield* fail("startup-current-admission-audit", "authority-divergent", admissionId);
        }
        if (current.status === "waiting" && chains.length !== 0) {
          return yield* fail("startup-waiting-history-audit", "authority-divergent", admissionId);
        }
        if (
          current.status === "admitted" &&
          (chains.length !== admissionChains.length ||
            currentAdmissionChain?.[0] === undefined ||
            !isNewest(currentAdmissionChain[0]))
        ) {
          return yield* fail("startup-admitted-history-audit", "authority-divergent", admissionId);
        }
        if (
          current.status === "entered" &&
          (entryChains.length < 1 ||
            quarantineChains.length !== 0 ||
            supersedeChains.length !== 0 ||
            releaseChains.length !== 0 ||
            !entryChains.some(
              (chain) =>
                chain.providerInstanceId === current.providerInstanceId &&
                chain.ownerId === current.ownerId &&
                chain.providerFenceToken === current.providerFenceToken &&
                isNewest(chain),
            ))
        ) {
          return yield* fail("startup-entered-history-audit", "authority-divergent", admissionId);
        }
        if (
          current.status === "quarantined" &&
          (entryChains.length < 1 ||
            quarantineChains.length !== 1 ||
            supersedeChains.length !== 0 ||
            releaseChains.length !== 0 ||
            !quarantineChains.some(
              (chain) =>
                chain.providerInstanceId === current.providerInstanceId &&
                chain.ownerId === current.ownerId &&
                chain.providerFenceToken === current.providerFenceToken &&
                isNewest(chain),
            ))
        ) {
          return yield* fail(
            "startup-quarantined-history-audit",
            "authority-divergent",
            admissionId,
          );
        }
        if (current.status !== "released" && current.status !== "superseded") continue;
        if (
          (current.status === "released" &&
            (releaseChains.length !== 1 || supersedeChains.length !== 0)) ||
          (current.status === "superseded" &&
            (supersedeChains.length !== 1 ||
              releaseChains.length !== 0 ||
              entryChains.length !== 0))
        ) {
          return yield* fail("startup-terminal-history-audit", "authority-divergent", admissionId);
        }
        const terminalChain =
          current.status === "released" ? releaseChains[0]! : supersedeChains[0]!;
        if (
          terminalChain.providerFenceToken !== (current.providerFenceToken ?? 0) ||
          !isNewest(terminalChain)
        ) {
          return yield* fail(
            "startup-terminal-authority-audit",
            "authority-divergent",
            admissionId,
          );
        }
        // Replaying the existing terminal authority is a read-only semantic
        // audit here: the complete-chain and fence checks above guarantee that
        // appendAuthority finds the single committed marker instead of writing.
        // This deliberately reuses the production release binding checks for
        // finalization, terminal runtime provenance, and the canonical payload.
        const releasedProvider = yield* releaseFromFinalizationInTransactionRaw({
          stage: current.stage,
          handoffId: current.handoffId,
          finalizedAt: terminalChain.occurredAt,
        });
        if (releasedProvider !== current.providerInstanceId) {
          return yield* fail(
            "startup-terminal-authority-audit",
            "authority-divergent",
            admissionId,
          );
        }
      }
    },
  );

  yield* auditStartupAuthority();

  return ProviderAdmissionStore.of({
    resume,
    request,
    admitOldest,
    validateAndEnterInTransaction,
    quarantine,
    quarantineIfEntered,
    recordUsage,
    listWaiting,
    listDueDeadlines,
    listEnteredWithoutRelease,
    minimumDeadline,
    minimumDeadlineAfter,
    releaseFromFinalizationInTransaction,
    catchUpFinalized,
  });
});

export const ProviderAdmissionStoreLive = Layer.effect(ProviderAdmissionStore, make);
