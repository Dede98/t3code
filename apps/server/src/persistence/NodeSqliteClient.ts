/**
 * Port of `@effect/sql-sqlite-node` that uses the native `node:sqlite`
 * bindings instead of `better-sqlite3`.
 *
 * @module SqliteClient
 */
import * as NodeSqlite from "node:sqlite";

import {
  AgentControlTaskFinalizedAfterVerificationPayload,
  AgentControlTaskState,
  AgentControlStageRunLeaseReleasedAfterVerificationPayloadStorage,
  AgentControlStageRunLeaseState,
  AgentControlStageRunState,
  AgentControlStageRunVerificationTerminalPayloadStorage,
  AgentControlVerificationStageFinalizationDocumentStorage,
} from "@t3tools/contracts";

import * as Cache from "effect/Cache";
import * as Config from "effect/Config";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { identity } from "effect/Function";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Context from "effect/Context";
import * as Stream from "effect/Stream";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
import * as Client from "effect/unstable/sql/SqlClient";
import type { Connection } from "effect/unstable/sql/SqlConnection";
import { SqlError, classifySqliteError } from "effect/unstable/sql/SqlError";
import * as Statement from "effect/unstable/sql/Statement";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseJsonStrict,
  sha256Utf8,
  type JsonValue,
} from "../agentControl/initialPlanning/eventEvidence.ts";
import { sha256AgentControlIdentity } from "../agentControl/controlledThreadReservation/identity.ts";
import {
  deriveAgentControlRunOnceId,
  deriveRunOnceClaimId,
  deriveRunOnceCommandId,
  deriveRunOnceEvidenceId,
  deriveRunOnceMarkerId,
  deriveRunOncePublicationId,
  deriveRunOnceReceiptId,
} from "../agentControl/runOnce/identity.ts";
import { fingerprintAgentControlRunOnceSource } from "../agentControl/runOnce/source.ts";

import { NodeSqliteTransactionHooks } from "./Services/NodeSqliteTransactionHooks.ts";
import {
  isFatalUtf8Blob,
  SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION,
  SQLITE_FATAL_UTF8_FUNCTION,
  SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_FUNCTION,
  SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_PROTOCOL_FUNCTION,
  SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
  SQLITE_VERIFICATION_COMPLETION_DIGEST_FUNCTION,
  SQLITE_VERIFICATION_DELTA_DIGEST_FUNCTION,
  SQLITE_VERIFICATION_EVIDENCE_DIGEST_FUNCTION,
  sqliteVerificationCompletionDigest,
  sqliteVerificationDeltaDigest,
  sqliteVerificationEvidenceDigest,
  sqliteOrchestrationEventAuthorityRoute,
  sqliteOrchestrationEventJsonStorage,
  sqliteOrchestrationEventJsonStorageProtocol,
  sqliteOrchestrationEventProjectMembershipRoute,
} from "./SqliteFunctions.ts";

export const NODE_SQLITE_FATAL_UTF8_FUNCTION = SQLITE_FATAL_UTF8_FUNCTION;
export const NODE_SQLITE_VERIFICATION_STAGE_TERMINAL_STORAGE_FUNCTION =
  "t3_verification_stage_terminal_storage";
export const NODE_SQLITE_VERIFICATION_LEASE_RELEASE_STORAGE_FUNCTION =
  "t3_verification_lease_release_storage";
export const NODE_SQLITE_VERIFICATION_FINALIZATION_DOCUMENT_STORAGE_FUNCTION =
  "t3_verification_finalization_document_storage";
export const NODE_SQLITE_VERIFICATION_FINALIZATION_PAYLOAD_MATCH_FUNCTION =
  "t3_verification_finalization_payload_match";
export const NODE_SQLITE_VERIFICATION_TERMINAL_PAYLOAD_PAIR_MATCH_FUNCTION =
  "t3_verification_terminal_payload_pair_match";
export const NODE_SQLITE_VERIFICATION_STAGE_PROJECTION_MATCH_FUNCTION =
  "t3_verification_stage_projection_match";
export const NODE_SQLITE_VERIFICATION_LEASE_PROJECTION_MATCH_FUNCTION =
  "t3_verification_lease_projection_match";
export const NODE_SQLITE_VERIFICATION_SOURCE_AUTHORITY_MATCH_FUNCTION =
  "t3_verification_source_authority_match";
export const NODE_SQLITE_TASK_VERIFICATION_FINALIZATION_PAYLOAD_STORAGE_FUNCTION =
  "t3_task_verification_finalization_payload_storage";
export const NODE_SQLITE_TASK_VERIFICATION_FINALIZATION_DOCUMENT_STORAGE_FUNCTION =
  "t3_task_verification_finalization_document_storage";
export const NODE_SQLITE_TASK_VERIFICATION_FINALIZATION_MARKER_MATCH_FUNCTION =
  "t3_task_verification_finalization_marker_match";
export const NODE_SQLITE_TASK_VERIFICATION_FINALIZATION_PROJECTION_MATCH_FUNCTION =
  "t3_task_verification_finalization_projection_match";
export const NODE_SQLITE_RUN_ONCE_CANONICAL_BLOB_MATCH_FUNCTION =
  "t3_run_once_canonical_blob_match";
export const NODE_SQLITE_RUN_ONCE_ACTIVATION_IDENTITY_MATCH_FUNCTION =
  "t3_run_once_activation_identity_match";
export const NODE_SQLITE_RUN_ONCE_STEP_IDENTITY_MATCH_FUNCTION = "t3_run_once_step_identity_match";
export const NODE_SQLITE_RUN_ONCE_MARKER_MATCH_FUNCTION = "t3_run_once_marker_match";
export const NODE_SQLITE_RUN_ONCE_SOURCE_FINGERPRINT_MATCH_FUNCTION =
  "t3_run_once_source_fingerprint_match";
export const NODE_SQLITE_RUN_ONCE_MODE_COMMAND_FINGERPRINT_MATCH_FUNCTION =
  "t3_run_once_mode_command_fingerprint_match";
export const NODE_SQLITE_RUN_ONCE_MODE_EVENT_MATCH_FUNCTION = "t3_run_once_mode_event_match";
export const NODE_SQLITE_RUN_ONCE_THREAD_ACTIVATION_IDENTITY_MATCH_FUNCTION =
  "t3_run_once_thread_activation_identity_match";

const runOnceCanonicalBlobMatch = (payloadBytes: unknown, fingerprint: unknown): number => {
  try {
    if (typeof fingerprint !== "string" || !/^[0-9a-f]{64}$/u.test(fingerprint)) return 0;
    const source = decodeCanonicalUtf8Bytes(payloadBytes);
    if (canonicalJson(parseJsonStrict(source)) !== source) return 0;
    return sha256Utf8(source) === fingerprint ? 1 : 0;
  } catch {
    return 0;
  }
};

const integer = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 1;

const runOnceActivationIdentityMatch = (
  actual: unknown,
  projectId: unknown,
  eventId: unknown,
  eventSequence: unknown,
  eventStreamVersion: unknown,
  commandId: unknown,
): number => {
  try {
    if (
      typeof actual !== "string" ||
      typeof projectId !== "string" ||
      typeof eventId !== "string" ||
      !integer(eventSequence) ||
      !integer(eventStreamVersion) ||
      typeof commandId !== "string"
    )
      return 0;
    return deriveAgentControlRunOnceId({
      projectId: projectId as never,
      activationEventId: eventId as never,
      activationEventSequence: eventSequence,
      activationEventStreamVersion: eventStreamVersion,
      activationCommandId: commandId as never,
    }) === actual
      ? 1
      : 0;
  } catch {
    return 0;
  }
};

const runOnceStepIdentityMatch = (
  kind: unknown,
  actual: unknown,
  runId: unknown,
  ordinal: unknown,
  step: unknown,
): number => {
  try {
    if (
      typeof kind !== "string" ||
      typeof actual !== "string" ||
      typeof runId !== "string" ||
      !integer(ordinal) ||
      typeof step !== "string"
    )
      return 0;
    const brandedRunId = runId as never;
    const expected =
      kind === "command"
        ? deriveRunOnceCommandId(brandedRunId, ordinal, step)
        : kind === "evidence"
          ? deriveRunOnceEvidenceId(brandedRunId, ordinal, step)
          : kind === "receipt"
            ? deriveRunOnceReceiptId(brandedRunId, ordinal, step)
            : kind === "marker"
              ? deriveRunOnceMarkerId(brandedRunId, ordinal, step)
              : kind === "claim"
                ? deriveRunOnceClaimId(brandedRunId, ordinal, step)
                : kind === "publication"
                  ? deriveRunOncePublicationId(brandedRunId, ordinal, step)
                  : null;
    return expected === actual ? 1 : 0;
  } catch {
    return 0;
  }
};

const runOnceMarkerMatch = (
  actual: unknown,
  runId: unknown,
  ordinal: unknown,
  step: unknown,
  evidenceId: unknown,
  receiptId: unknown,
  markerId: unknown,
  commandId: unknown,
  payloadFingerprint: unknown,
  recordedAt: unknown,
): number => {
  try {
    if (
      typeof actual !== "string" ||
      typeof runId !== "string" ||
      !integer(ordinal) ||
      typeof step !== "string" ||
      typeof evidenceId !== "string" ||
      typeof receiptId !== "string" ||
      typeof markerId !== "string" ||
      typeof commandId !== "string" ||
      typeof payloadFingerprint !== "string" ||
      typeof recordedAt !== "string"
    )
      return 0;
    const expected = sha256Utf8(
      canonicalJson({
        commandId,
        domain: "agent-control-run-once-step-marker-v1",
        evidenceId,
        markerId,
        ordinal,
        payloadFingerprint,
        receiptId,
        recordedAt,
        runId,
        step,
      }),
    );
    return expected === actual ? 1 : 0;
  } catch {
    return 0;
  }
};

const runOnceSourceFingerprintMatch = (
  actual: unknown,
  projectId: unknown,
  githubIntakeSequence: unknown,
  githubProjectionRevision: unknown,
  githubConfigRevision: unknown,
  repositoryNodeId: unknown,
  expectedIssueCount: unknown,
): number => {
  try {
    if (
      typeof actual !== "string" ||
      typeof projectId !== "string" ||
      !integer(githubIntakeSequence) ||
      !integer(githubProjectionRevision) ||
      !integer(githubConfigRevision) ||
      typeof repositoryNodeId !== "string" ||
      typeof expectedIssueCount !== "number" ||
      !Number.isSafeInteger(expectedIssueCount) ||
      expectedIssueCount < 0
    )
      return 0;
    const expected = fingerprintAgentControlRunOnceSource({
      schemaVersion: 1,
      projectId: projectId as never,
      githubIntakeSequence,
      githubProjectionRevision,
      githubConfigRevision,
      repositoryNodeId,
      pollStatus: "success",
      expectedIssueCount,
    });
    return expected === actual ? 1 : 0;
  } catch {
    return 0;
  }
};

const runOnceModeCommandFingerprintMatch = (
  actual: unknown,
  commandId: unknown,
  projectId: unknown,
  expectedRevision: unknown,
  mode: unknown,
  runOnceTaskId: unknown = undefined,
): number => {
  if (
    typeof actual !== "string" ||
    typeof commandId !== "string" ||
    typeof projectId !== "string" ||
    typeof expectedRevision !== "number" ||
    !Number.isSafeInteger(expectedRevision) ||
    expectedRevision < 0 ||
    typeof mode !== "string" ||
    (runOnceTaskId != null &&
      (typeof runOnceTaskId !== "string" ||
        runOnceTaskId.trim() !== runOnceTaskId ||
        runOnceTaskId.length === 0 ||
        mode !== "run-once"))
  ) {
    return 0;
  }
  const canonical = [
    "agentControl.project.mode.set",
    commandId,
    projectId,
    String(expectedRevision),
    mode,
    ...(typeof runOnceTaskId === "string" ? [runOnceTaskId] : []),
  ]
    .map((part) => `${part.length}:${part}`)
    .join("");
  return sha256Utf8(canonical) === actual ? 1 : 0;
};

const runOnceModeEventMatch = (
  payloadBytes: unknown,
  metadataBytes: unknown,
  projectId: unknown,
  previousMode: unknown,
  mode: unknown,
  previousPausedFromMode: unknown,
  pausedFromMode: unknown,
  changedAt: unknown,
  runOnceTaskId: unknown = undefined,
): number => {
  try {
    if (
      typeof projectId !== "string" ||
      typeof previousMode !== "string" ||
      typeof mode !== "string" ||
      (previousPausedFromMode !== null && typeof previousPausedFromMode !== "string") ||
      (pausedFromMode !== null && typeof pausedFromMode !== "string") ||
      typeof changedAt !== "string" ||
      (runOnceTaskId != null &&
        (typeof runOnceTaskId !== "string" ||
          runOnceTaskId.trim() !== runOnceTaskId ||
          runOnceTaskId.length === 0 ||
          mode !== "run-once" ||
          previousMode !== "observe" ||
          previousPausedFromMode !== null ||
          pausedFromMode !== null))
    ) {
      return 0;
    }
    return canonicalJson(parseJsonStrict(decodeCanonicalUtf8Bytes(payloadBytes))) ===
      canonicalJson({
        changedAt,
        mode,
        pausedFromMode,
        previousMode,
        previousPausedFromMode,
        projectId,
        ...(typeof runOnceTaskId === "string" ? { runOnceTaskId } : {}),
      }) &&
      canonicalJson(parseJsonStrict(decodeCanonicalUtf8Bytes(metadataBytes))) ===
        canonicalJson({ schemaVersion: 1 })
      ? 1
      : 0;
  } catch {
    return 0;
  }
};

const runOnceThreadActivationIdentityMatch = (
  actual: unknown,
  prepareCommandId: unknown,
  controlledThreadReservationId: unknown,
): number => {
  if (
    typeof actual !== "string" ||
    typeof prepareCommandId !== "string" ||
    typeof controlledThreadReservationId !== "string"
  ) {
    return 0;
  }
  const expected = `controlled-thread-activation-${sha256AgentControlIdentity([
    "agent-control-controlled-thread-activation-v1",
    prepareCommandId,
    controlledThreadReservationId,
  ])}`;
  return actual === expected ? 1 : 0;
};

const decodeVerificationStageTerminal = Schema.decodeUnknownSync(
  AgentControlStageRunVerificationTerminalPayloadStorage,
);
const decodeVerificationLeaseRelease = Schema.decodeUnknownSync(
  AgentControlStageRunLeaseReleasedAfterVerificationPayloadStorage,
);
const decodeVerificationFinalizationDocument = Schema.decodeUnknownSync(
  AgentControlVerificationStageFinalizationDocumentStorage,
);
const decodeVerificationStageState = Schema.decodeUnknownSync(
  AgentControlStageRunState.annotate({ parseOptions: { onExcessProperty: "error" } }),
);
const decodeVerificationLeaseState = Schema.decodeUnknownSync(
  AgentControlStageRunLeaseState.annotate({ parseOptions: { onExcessProperty: "error" } }),
);
const decodeVerificationMetadata = Schema.decodeUnknownSync(
  Schema.Struct({ schemaVersion: Schema.Literal(1) }).annotate({
    parseOptions: { onExcessProperty: "error" },
  }),
);
const VerificationFinalizationSourceAuthority = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  projectId: Schema.String,
  taskId: Schema.String,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  taskRevision: Schema.Number,
  githubIntakeSequence: Schema.Number,
  sourceIdentityFingerprint: Schema.String,
  admissionEvidenceId: Schema.String,
  admissionReceiptId: Schema.String,
  admissionMarkerId: Schema.String,
  materializationEvidenceId: Schema.String,
  materializationReceiptId: Schema.String,
  materializationMarkerId: Schema.String,
  startEvidenceId: Schema.String,
  startReceiptId: Schema.String,
  startMarkerId: Schema.String,
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  controlledThreadReservationId: Schema.String,
  threadId: Schema.String,
  planningThreadId: Schema.String,
  planId: Schema.String,
  proposedPlanDigest: Schema.String,
  providerDeliveryId: Schema.String,
  deliveryRevision: Schema.Number,
  claimGeneration: Schema.Number,
  attemptCount: Schema.Number,
  providerInstanceId: Schema.String,
  providerTurnId: Schema.String,
  runtimeMode: Schema.Literal("approval-required"),
  modelSelectionFingerprint: Schema.String,
  leaseId: Schema.String,
  leaseHolderId: Schema.String,
  fenceToken: Schema.Number,
  deliveryTerminalState: Schema.Literals(["completed", "failed", "interrupted"]),
  terminalRuntimeEventId: Schema.String,
  terminalAt: Schema.String,
}).annotate({ parseOptions: { onExcessProperty: "error" } });
const decodeVerificationFinalizationSourceAuthority = Schema.decodeUnknownSync(
  VerificationFinalizationSourceAuthority,
);
const TaskVerificationFinalizationDocumentStorage = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  commandId: Schema.String,
  taskFinalizationEvidenceId: Schema.String,
  taskFinalizationReceiptId: Schema.String,
  taskFinalizationMarkerId: Schema.String,
  verificationFinalizationEvidenceId: Schema.String,
  verificationFinalizationReceiptId: Schema.String,
  verificationFinalizationMarkerId: Schema.String,
  verificationFinalizationCommandId: Schema.String,
  verificationFinalizationFingerprint: Schema.String,
  verificationFinalizationMarkerFingerprint: Schema.String,
  taskEventId: Schema.String,
  taskEventStreamVersion: Schema.Int,
  payload: AgentControlTaskFinalizedAfterVerificationPayload,
  finalizedAt: Schema.String,
}).annotate({ parseOptions: { onExcessProperty: "error" } });
const decodeTaskVerificationFinalizationPayload = Schema.decodeUnknownSync(
  AgentControlTaskFinalizedAfterVerificationPayload.annotate({
    parseOptions: { onExcessProperty: "error" },
  }),
);
const decodeTaskVerificationFinalizationDocument = Schema.decodeUnknownSync(
  TaskVerificationFinalizationDocumentStorage,
);
const decodeTaskVerificationState = Schema.decodeUnknownSync(
  AgentControlTaskState.annotate({ parseOptions: { onExcessProperty: "error" } }),
);

const decodeStrictStorageJson = <A>(
  bytes: unknown,
  decode: (input: unknown) => A,
): { readonly source: string; readonly value: A } => {
  const source = decodeCanonicalUtf8Bytes(bytes);
  if (source.includes("\0")) throw new Error("NUL is not valid in verification authority JSON");
  const parsed = parseJsonStrict(source);
  const value = decode(parsed);
  if (JSON.stringify(value) !== source && canonicalJson(value as unknown as JsonValue) !== source) {
    throw new Error("Verification authority JSON was transformed by typed decoding");
  }
  return { source, value };
};

const decodeTypedStorageJson = <A>(bytes: unknown, decode: (input: unknown) => A): A => {
  const source = decodeCanonicalUtf8Bytes(bytes);
  if (source.includes("\0")) throw new Error("NUL is not valid in Task authority JSON");
  const parsed = parseJsonStrict(source);
  const value = decode(parsed);
  if (
    canonicalJson(value as unknown as JsonValue) !== canonicalJson(parsed as unknown as JsonValue)
  ) {
    throw new Error("Task authority JSON was transformed by typed decoding");
  }
  return value;
};

const taskVerificationUtf8Encoder = new TextEncoder();
const isTaskVerificationNonEmptyString = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value.trim() === value && !value.includes("\0");
const isTaskVerificationSha256 = (value: unknown): value is string =>
  typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
const taskVerificationIdentity = (
  prefix: string,
  domain: string,
  parts: ReadonlyArray<string>,
): string =>
  `${prefix}-${sha256Utf8(
    canonicalJson({ domain: `agent-control-task-${domain}-v1`, parts } as unknown as JsonValue),
  )}`;

type TaskVerificationFinalizationPayloadValue = Schema.Schema.Type<
  typeof AgentControlTaskFinalizedAfterVerificationPayload
>;

const taskVerificationFinalizationIdentity = (
  payload: TaskVerificationFinalizationPayloadValue,
) => {
  const parts = [
    payload.verificationFinalizationMarkerId,
    payload.taskId,
    String(payload.verificationTaskRevision),
  ];
  return {
    commandId: taskVerificationIdentity(
      "task-verification-finalization",
      "verification-finalization-command",
      parts,
    ),
    evidenceId: taskVerificationIdentity(
      "task-verification-finalization-evidence",
      "verification-finalization-evidence",
      parts,
    ),
    receiptId: taskVerificationIdentity(
      "task-verification-finalization-receipt",
      "verification-finalization-receipt",
      parts,
    ),
    markerId: taskVerificationIdentity(
      "task-verification-finalization-marker",
      "verification-finalization-marker",
      parts,
    ),
    eventId: taskVerificationIdentity(
      "task-finalized-after-verification-event",
      "finalized-after-verification-event",
      parts,
    ),
  } as const;
};

const validTaskVerificationFinalizationPayload = (
  payload: TaskVerificationFinalizationPayloadValue,
): boolean => {
  if (
    payload.verificationTaskRevision < 1 ||
    payload.previousTaskRevision < payload.verificationTaskRevision ||
    payload.githubIntakeSequence < 1 ||
    payload.taskSourceEventSequence < 1 ||
    payload.taskSourceEventStreamVersion !== payload.verificationTaskRevision ||
    payload.terminalStageEventSequence < 1 ||
    payload.terminalStageEventStreamVersion !== 3 ||
    payload.releasedLeaseEventSequence < 1 ||
    payload.releasedLeaseEventStreamVersion < 2 ||
    !isTaskVerificationSha256(payload.sourceIdentityFingerprint) ||
    !isTaskVerificationSha256(payload.handoffFingerprint) ||
    !isTaskVerificationSha256(payload.verificationFinalizationFingerprint) ||
    !isTaskVerificationSha256(payload.verificationFinalizationMarkerFingerprint)
  ) {
    return false;
  }
  const identity = taskVerificationFinalizationIdentity(payload);
  return (
    payload.taskFinalizationEvidenceId === identity.evidenceId &&
    [
      payload.projectId,
      payload.taskId,
      payload.taskSourceEventId,
      payload.handoffId,
      payload.verificationFinalizationEvidenceId,
      payload.verificationFinalizationReceiptId,
      payload.verificationFinalizationMarkerId,
      payload.verificationFinalizationCommandId,
      payload.terminalStageRunId,
      payload.terminalStageEventId,
      payload.releasedLeaseId,
      payload.releasedLeaseEventId,
      payload.terminalRuntimeEventId,
      payload.finalizedAt,
    ].every(isTaskVerificationNonEmptyString)
  );
};

const taskVerificationFinalizationPayloadStorage = (
  eventType: unknown,
  payloadBytes: unknown,
  metadataBytes: unknown,
  eventId: unknown,
  eventStreamVersion: unknown,
  commandId: unknown,
): Uint8Array | null => {
  try {
    if (eventType !== "agentControl.task.finalizedAfterVerification") return null;
    const payload = decodeStrictStorageJson(
      payloadBytes,
      decodeTaskVerificationFinalizationPayload,
    );
    decodeStrictStorageJson(metadataBytes, decodeVerificationMetadata);
    if (!validTaskVerificationFinalizationPayload(payload.value)) return null;
    const identity = taskVerificationFinalizationIdentity(payload.value);
    if (
      eventId !== identity.eventId ||
      eventStreamVersion !== payload.value.previousTaskRevision + 1 ||
      commandId !== identity.commandId
    ) {
      return null;
    }
    return taskVerificationUtf8Encoder.encode(payload.source);
  } catch {
    return null;
  }
};

const taskVerificationFinalizationDocumentStorage = (
  documentBytes: unknown,
  payloadBytes: unknown,
  eventId: unknown,
  eventStreamVersion: unknown,
  commandId: unknown,
  evidenceId: unknown,
  receiptId: unknown,
  markerId: unknown,
  finalizationFingerprint: unknown,
  projectId: unknown,
  taskId: unknown,
  verificationTaskRevision: unknown,
  previousTaskRevision: unknown,
  githubIntakeSequence: unknown,
  sourceIdentityFingerprint: unknown,
  taskSourceEventId: unknown,
  taskSourceEventSequence: unknown,
  taskSourceEventStreamVersion: unknown,
): Uint8Array | null => {
  try {
    if (
      !isTaskVerificationNonEmptyString(eventId) ||
      typeof eventStreamVersion !== "number" ||
      !Number.isSafeInteger(eventStreamVersion) ||
      eventStreamVersion < 2 ||
      !isTaskVerificationNonEmptyString(commandId) ||
      !isTaskVerificationNonEmptyString(evidenceId) ||
      !isTaskVerificationNonEmptyString(receiptId) ||
      !isTaskVerificationNonEmptyString(markerId) ||
      !isTaskVerificationSha256(finalizationFingerprint)
    ) {
      return null;
    }
    const document = decodeStrictStorageJson(
      documentBytes,
      decodeTaskVerificationFinalizationDocument,
    );
    const payload = decodeStrictStorageJson(
      payloadBytes,
      decodeTaskVerificationFinalizationPayload,
    );
    if (!validTaskVerificationFinalizationPayload(payload.value)) return null;
    const identity = taskVerificationFinalizationIdentity(payload.value);
    if (
      document.value.commandId !== identity.commandId ||
      document.value.taskFinalizationEvidenceId !== identity.evidenceId ||
      document.value.taskFinalizationReceiptId !== identity.receiptId ||
      document.value.taskFinalizationMarkerId !== identity.markerId ||
      document.value.taskEventId !== identity.eventId ||
      document.value.taskEventStreamVersion !== payload.value.previousTaskRevision + 1 ||
      document.value.verificationFinalizationEvidenceId !==
        payload.value.verificationFinalizationEvidenceId ||
      document.value.verificationFinalizationReceiptId !==
        payload.value.verificationFinalizationReceiptId ||
      document.value.verificationFinalizationMarkerId !==
        payload.value.verificationFinalizationMarkerId ||
      document.value.verificationFinalizationCommandId !==
        payload.value.verificationFinalizationCommandId ||
      document.value.verificationFinalizationFingerprint !==
        payload.value.verificationFinalizationFingerprint ||
      document.value.verificationFinalizationMarkerFingerprint !==
        payload.value.verificationFinalizationMarkerFingerprint ||
      document.value.finalizedAt !== payload.value.finalizedAt ||
      canonicalJson(document.value.payload as unknown as JsonValue) !==
        canonicalJson(payload.value as unknown as JsonValue) ||
      eventId !== identity.eventId ||
      eventStreamVersion !== document.value.taskEventStreamVersion ||
      commandId !== identity.commandId ||
      evidenceId !== identity.evidenceId ||
      receiptId !== identity.receiptId ||
      markerId !== identity.markerId ||
      finalizationFingerprint !== sha256Utf8(document.source) ||
      projectId !== payload.value.projectId ||
      taskId !== payload.value.taskId ||
      verificationTaskRevision !== payload.value.verificationTaskRevision ||
      previousTaskRevision !== payload.value.previousTaskRevision ||
      githubIntakeSequence !== payload.value.githubIntakeSequence ||
      sourceIdentityFingerprint !== payload.value.sourceIdentityFingerprint ||
      taskSourceEventId !== payload.value.taskSourceEventId ||
      taskSourceEventSequence !== payload.value.taskSourceEventSequence ||
      taskSourceEventStreamVersion !== payload.value.taskSourceEventStreamVersion
    ) {
      return null;
    }
    return taskVerificationUtf8Encoder.encode(document.source);
  } catch {
    return null;
  }
};

const taskVerificationFinalizationMarkerMatch = (
  documentBytes: unknown,
  markerFingerprint: unknown,
): number => {
  try {
    if (!isTaskVerificationSha256(markerFingerprint)) return 0;
    const document = decodeStrictStorageJson(
      documentBytes,
      decodeTaskVerificationFinalizationDocument,
    );
    const finalizationFingerprint = sha256Utf8(document.source);
    const expected = sha256Utf8(
      canonicalJson({
        domain: "agent-control-task-verification-finalization-marker-v1",
        evidenceId: document.value.taskFinalizationEvidenceId,
        receiptId: document.value.taskFinalizationReceiptId,
        markerId: document.value.taskFinalizationMarkerId,
        commandId: document.value.commandId,
        finalizationFingerprint,
        verificationMarkerId: document.value.verificationFinalizationMarkerId,
        eventId: document.value.taskEventId,
        finalizedAt: document.value.finalizedAt,
      } as unknown as JsonValue),
    );
    return markerFingerprint === expected ? 1 : 0;
  } catch {
    return 0;
  }
};

const taskVerificationFinalizationProjectionMatch = (
  oldStateBytes: unknown,
  newStateBytes: unknown,
  payloadBytes: unknown,
  eventSequence: unknown,
): number => {
  try {
    if (!(oldStateBytes instanceof Uint8Array) || !(newStateBytes instanceof Uint8Array)) return 0;
    if (!(payloadBytes instanceof Uint8Array)) return 0;
    if (typeof eventSequence !== "number" || !Number.isSafeInteger(eventSequence)) return 0;
    const oldState = decodeTypedStorageJson(oldStateBytes, decodeTaskVerificationState);
    const newState = decodeTypedStorageJson(newStateBytes, decodeTaskVerificationState);
    const payload = decodeStrictStorageJson(
      payloadBytes,
      decodeTaskVerificationFinalizationPayload,
    ).value;
    const expected = {
      ...oldState,
      status: payload.status,
      stage: "verification" as const,
      updatedAt: payload.finalizedAt,
      revision: oldState.revision + 1,
      sequence: eventSequence,
    };
    return payload.taskId === oldState.taskId &&
      payload.previousTaskRevision === oldState.revision &&
      payload.previousStatus === oldState.status &&
      payload.finalizedAt === newState.updatedAt &&
      canonicalJson(newState as unknown as JsonValue) ===
        canonicalJson(expected as unknown as JsonValue)
      ? 1
      : 0;
  } catch {
    return 0;
  }
};

const verificationStageTerminalStorage = (
  eventType: unknown,
  payloadBytes: unknown,
  metadataBytes: unknown,
): number => {
  try {
    if (typeof eventType !== "string") return 0;
    const payload = decodeStrictStorageJson(payloadBytes, decodeVerificationStageTerminal).value;
    decodeStrictStorageJson(metadataBytes, decodeVerificationMetadata);
    const expectedType =
      payload.status === "succeeded"
        ? "agentControl.stageRun.verificationSucceeded"
        : payload.status === "failed"
          ? "agentControl.stageRun.verificationFailed"
          : "agentControl.stageRun.verificationCancelled";
    return eventType === expectedType ? 1 : 0;
  } catch {
    return 0;
  }
};

const verificationLeaseReleaseStorage = (
  eventType: unknown,
  payloadBytes: unknown,
  metadataBytes: unknown,
): number => {
  try {
    if (eventType !== "agentControl.stageRunLease.releasedAfterVerification") return 0;
    decodeStrictStorageJson(payloadBytes, decodeVerificationLeaseRelease);
    decodeStrictStorageJson(metadataBytes, decodeVerificationMetadata);
    return 1;
  } catch {
    return 0;
  }
};

const verificationFinalizationDocumentStorage = (documentBytes: unknown): number => {
  try {
    decodeStrictStorageJson(documentBytes, decodeVerificationFinalizationDocument);
    return 1;
  } catch {
    return 0;
  }
};

const verificationFinalizationPayloadMatch = (
  stagePayloadBytes: unknown,
  leasePayloadBytes: unknown,
  documentBytes: unknown,
): number => {
  try {
    const stage = decodeStrictStorageJson(stagePayloadBytes, decodeVerificationStageTerminal).value;
    const lease = decodeStrictStorageJson(leasePayloadBytes, decodeVerificationLeaseRelease).value;
    const document = decodeStrictStorageJson(
      documentBytes,
      decodeVerificationFinalizationDocument,
    ).value;
    const sharedKeys = [
      "projectId",
      "taskId",
      "stageRunId",
      "attemptId",
      "taskRevision",
      "githubIntakeSequence",
      "sourceIdentityFingerprint",
      "admissionEvidenceId",
      "admissionReceiptId",
      "admissionMarkerId",
      "materializationEvidenceId",
      "materializationReceiptId",
      "materializationMarkerId",
      "startEvidenceId",
      "startReceiptId",
      "startMarkerId",
      "handoffId",
      "handoffFingerprint",
      "controlledThreadReservationId",
      "threadId",
      "planningThreadId",
      "planId",
      "proposedPlanDigest",
      "providerDeliveryId",
      "deliveryRevision",
      "providerInstanceId",
      "providerTurnId",
      "runtimeMode",
      "modelSelectionFingerprint",
      "terminalRuntimeEventId",
      "finalizationEvidenceId",
    ] as const;
    if (sharedKeys.some((key) => stage[key] !== lease[key])) return 0;
    if (
      stage.leaseId !== lease.leaseId ||
      stage.leaseHolderId !== lease.holderId ||
      stage.fenceToken !== lease.fenceToken ||
      stage.deliveryTerminalState !== lease.deliveryTerminalState ||
      stage.terminalCause !== lease.terminalCause ||
      stage.status !== lease.stageStatus ||
      stage.finalizedAt !== lease.releasedAt ||
      JSON.stringify(stage.evaluation) !== JSON.stringify(lease.evaluation) ||
      document.handoffId !== stage.handoffId ||
      document.handoffFingerprint !== stage.handoffFingerprint ||
      document.finalizationEvidenceId !== stage.finalizationEvidenceId ||
      document.outcome !== stage.status ||
      document.terminalCause !== stage.terminalCause ||
      document.deliveryTerminalState !== stage.deliveryTerminalState ||
      document.terminalRuntimeEventId !== stage.terminalRuntimeEventId ||
      JSON.stringify(document.evaluation) !== JSON.stringify(stage.evaluation) ||
      document.stageEventId !== lease.stageEventId ||
      document.finalizedAt !== stage.finalizedAt ||
      JSON.stringify(document.stagePayload) !== JSON.stringify(stage) ||
      JSON.stringify(document.leasePayload) !== JSON.stringify(lease)
    ) {
      return 0;
    }
    return 1;
  } catch {
    return 0;
  }
};

const verificationTerminalPayloadPairMatch = (
  stagePayloadBytes: unknown,
  leasePayloadBytes: unknown,
): number => {
  try {
    const stage = decodeStrictStorageJson(stagePayloadBytes, decodeVerificationStageTerminal).value;
    const lease = decodeStrictStorageJson(leasePayloadBytes, decodeVerificationLeaseRelease).value;
    const sharedKeys = [
      "projectId",
      "taskId",
      "stageRunId",
      "attemptId",
      "taskRevision",
      "githubIntakeSequence",
      "sourceIdentityFingerprint",
      "admissionEvidenceId",
      "admissionReceiptId",
      "admissionMarkerId",
      "materializationEvidenceId",
      "materializationReceiptId",
      "materializationMarkerId",
      "startEvidenceId",
      "startReceiptId",
      "startMarkerId",
      "handoffId",
      "handoffFingerprint",
      "controlledThreadReservationId",
      "threadId",
      "planningThreadId",
      "planId",
      "proposedPlanDigest",
      "providerDeliveryId",
      "deliveryRevision",
      "providerInstanceId",
      "providerTurnId",
      "runtimeMode",
      "modelSelectionFingerprint",
      "terminalRuntimeEventId",
      "finalizationEvidenceId",
    ] as const;
    return sharedKeys.every((key) => stage[key] === lease[key]) &&
      stage.leaseId === lease.leaseId &&
      stage.leaseHolderId === lease.holderId &&
      stage.fenceToken === lease.fenceToken &&
      stage.deliveryTerminalState === lease.deliveryTerminalState &&
      stage.terminalCause === lease.terminalCause &&
      stage.status === lease.stageStatus &&
      stage.finalizedAt === lease.releasedAt &&
      JSON.stringify(stage.evaluation) === JSON.stringify(lease.evaluation)
      ? 1
      : 0;
  } catch {
    return 0;
  }
};

type VerificationFinalizationSourceAuthorityValue = Schema.Schema.Type<
  typeof VerificationFinalizationSourceAuthority
>;

const verificationPayloadMatchesSourceAuthority = (
  payload: Readonly<Record<string, unknown>>,
  source: VerificationFinalizationSourceAuthorityValue,
): boolean => {
  const expected = {
    projectId: source.projectId,
    taskId: source.taskId,
    stageRunId: source.stageRunId,
    attemptId: source.attemptId,
    taskRevision: source.taskRevision,
    githubIntakeSequence: source.githubIntakeSequence,
    sourceIdentityFingerprint: source.sourceIdentityFingerprint,
    admissionEvidenceId: source.admissionEvidenceId,
    admissionReceiptId: source.admissionReceiptId,
    admissionMarkerId: source.admissionMarkerId,
    materializationEvidenceId: source.materializationEvidenceId,
    materializationReceiptId: source.materializationReceiptId,
    materializationMarkerId: source.materializationMarkerId,
    startEvidenceId: source.startEvidenceId,
    startReceiptId: source.startReceiptId,
    startMarkerId: source.startMarkerId,
    handoffId: source.handoffId,
    handoffFingerprint: source.handoffFingerprint,
    controlledThreadReservationId: source.controlledThreadReservationId,
    threadId: source.threadId,
    planningThreadId: source.planningThreadId,
    planId: source.planId,
    proposedPlanDigest: source.proposedPlanDigest,
    providerDeliveryId: source.providerDeliveryId,
    deliveryRevision: source.deliveryRevision,
    providerInstanceId: source.providerInstanceId,
    providerTurnId: source.providerTurnId,
    runtimeMode: source.runtimeMode,
    modelSelectionFingerprint: source.modelSelectionFingerprint,
    leaseId: source.leaseId,
    fenceToken: source.fenceToken,
    deliveryTerminalState: source.deliveryTerminalState,
    terminalRuntimeEventId: source.terminalRuntimeEventId,
  } as const;
  if (Object.entries(expected).some(([key, value]) => payload[key] !== value)) return false;
  const holder = "leaseHolderId" in payload ? payload.leaseHolderId : payload.holderId;
  const finalizedAt = "finalizedAt" in payload ? payload.finalizedAt : payload.releasedAt;
  if (holder !== source.leaseHolderId || finalizedAt !== source.terminalAt) return false;
  if ("claimGeneration" in payload && payload.claimGeneration !== source.claimGeneration)
    return false;
  if ("attemptCount" in payload && payload.attemptCount !== source.attemptCount) return false;
  return true;
};

const verificationSourceAuthorityMatch = (
  payloadBytes: unknown,
  sourceAuthorityBytes: unknown,
): number => {
  try {
    const source = decodeStrictStorageJson(
      sourceAuthorityBytes,
      decodeVerificationFinalizationSourceAuthority,
    ).value;
    try {
      const stage = decodeStrictStorageJson(payloadBytes, decodeVerificationStageTerminal).value;
      return verificationPayloadMatchesSourceAuthority(stage, source) ? 1 : 0;
    } catch {
      // Continue with the other two closed payload families.
    }
    try {
      const lease = decodeStrictStorageJson(payloadBytes, decodeVerificationLeaseRelease).value;
      return verificationPayloadMatchesSourceAuthority(lease, source) ? 1 : 0;
    } catch {
      // Continue with the finalization document.
    }
    const document = decodeStrictStorageJson(
      payloadBytes,
      decodeVerificationFinalizationDocument,
    ).value;
    return verificationPayloadMatchesSourceAuthority(document.stagePayload, source) &&
      verificationPayloadMatchesSourceAuthority(document.leasePayload, source)
      ? 1
      : 0;
  } catch {
    return 0;
  }
};

const verificationStageProjectionMatch = (
  payloadBytes: unknown,
  stateBytes: unknown,
  projectId: unknown,
  taskId: unknown,
  stageRunId: unknown,
  attemptId: unknown,
  roleId: unknown,
  stageKind: unknown,
  stageOrdinal: unknown,
  attemptOrdinal: unknown,
  taskRevision: unknown,
  githubIntakeSequence: unknown,
  sourceIdentityFingerprint: unknown,
  status: unknown,
  createdAt: unknown,
  updatedAt: unknown,
  revision: unknown,
  sequence: unknown,
): number => {
  try {
    const payload = decodeStrictStorageJson(payloadBytes, decodeVerificationStageTerminal).value;
    const state = decodeStrictStorageJson(stateBytes, decodeVerificationStageState).value;
    return state.projectId === projectId &&
      state.taskId === taskId &&
      state.stageRunId === stageRunId &&
      state.attemptId === attemptId &&
      state.roleId === roleId &&
      state.stageKind === stageKind &&
      state.stageOrdinal === stageOrdinal &&
      state.attemptOrdinal === attemptOrdinal &&
      state.taskRevision === taskRevision &&
      state.githubIntakeSequence === githubIntakeSequence &&
      state.sourceIdentityFingerprint === sourceIdentityFingerprint &&
      state.status === status &&
      state.createdAt === createdAt &&
      state.updatedAt === updatedAt &&
      state.revision === revision &&
      state.sequence === sequence &&
      state.stageKind === "verification" &&
      state.roleId === "verifier" &&
      (state.stageOrdinal === 3 || state.stageOrdinal === 5) &&
      state.stageOrdinal === payload.stageOrdinal &&
      state.attemptOrdinal === 1 &&
      state.revision === 3 &&
      state.projectId === payload.projectId &&
      state.taskId === payload.taskId &&
      state.stageRunId === payload.stageRunId &&
      state.attemptId === payload.attemptId &&
      state.taskRevision === payload.taskRevision &&
      state.githubIntakeSequence === payload.githubIntakeSequence &&
      state.sourceIdentityFingerprint === payload.sourceIdentityFingerprint &&
      state.status === payload.status &&
      state.updatedAt === payload.finalizedAt
      ? 1
      : 0;
  } catch {
    return 0;
  }
};

const verificationLeaseProjectionMatch = (
  payloadBytes: unknown,
  stateBytes: unknown,
  leaseId: unknown,
  projectId: unknown,
  taskId: unknown,
  stageRunId: unknown,
  attemptId: unknown,
  taskRevision: unknown,
  githubIntakeSequence: unknown,
  sourceIdentityFingerprint: unknown,
  holderId: unknown,
  fenceToken: unknown,
  status: unknown,
  acquiredAt: unknown,
  renewedAt: unknown,
  expiresAt: unknown,
  releasedAt: unknown,
  revision: unknown,
  sequence: unknown,
): number => {
  try {
    const payload = decodeStrictStorageJson(payloadBytes, decodeVerificationLeaseRelease).value;
    const state = decodeStrictStorageJson(stateBytes, decodeVerificationLeaseState).value;
    return state.leaseId === leaseId &&
      state.projectId === projectId &&
      state.taskId === taskId &&
      state.stageRunId === stageRunId &&
      state.attemptId === attemptId &&
      state.taskRevision === taskRevision &&
      state.githubIntakeSequence === githubIntakeSequence &&
      state.sourceIdentityFingerprint === sourceIdentityFingerprint &&
      state.holderId === holderId &&
      state.fenceToken === fenceToken &&
      state.status === status &&
      state.acquiredAt === acquiredAt &&
      state.renewedAt === renewedAt &&
      state.expiresAt === expiresAt &&
      state.releasedAt === releasedAt &&
      state.revision === revision &&
      state.sequence === sequence &&
      state.status === "released" &&
      state.leaseId === payload.leaseId &&
      state.projectId === payload.projectId &&
      state.taskId === payload.taskId &&
      state.stageRunId === payload.stageRunId &&
      state.attemptId === payload.attemptId &&
      state.taskRevision === payload.taskRevision &&
      state.githubIntakeSequence === payload.githubIntakeSequence &&
      state.sourceIdentityFingerprint === payload.sourceIdentityFingerprint &&
      state.holderId === payload.holderId &&
      state.fenceToken === payload.fenceToken &&
      state.releasedAt === payload.releasedAt
      ? 1
      : 0;
  } catch {
    return 0;
  }
};

/** Register deterministic functions required by durable MAIN-schema write boundaries. */
export const registerNodeSqliteFunctions = (database: NodeSqlite.DatabaseSync): void => {
  database.function(NODE_SQLITE_FATAL_UTF8_FUNCTION, { deterministic: true }, isFatalUtf8Blob);
  database.function(
    SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_FUNCTION,
    { deterministic: true },
    sqliteOrchestrationEventJsonStorage,
  );
  database.function(
    SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_PROTOCOL_FUNCTION,
    { deterministic: true },
    sqliteOrchestrationEventJsonStorageProtocol,
  );
  database.function(
    SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION,
    { deterministic: true },
    sqliteOrchestrationEventAuthorityRoute,
  );
  database.function(
    SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
    { deterministic: true },
    sqliteOrchestrationEventProjectMembershipRoute,
  );
  database.function(
    SQLITE_VERIFICATION_DELTA_DIGEST_FUNCTION,
    { deterministic: true },
    sqliteVerificationDeltaDigest,
  );
  database.function(
    SQLITE_VERIFICATION_COMPLETION_DIGEST_FUNCTION,
    { deterministic: true },
    sqliteVerificationCompletionDigest,
  );
  database.function(
    SQLITE_VERIFICATION_EVIDENCE_DIGEST_FUNCTION,
    { deterministic: true },
    sqliteVerificationEvidenceDigest,
  );
  database.function(
    NODE_SQLITE_VERIFICATION_STAGE_TERMINAL_STORAGE_FUNCTION,
    { deterministic: true },
    verificationStageTerminalStorage,
  );
  database.function(
    NODE_SQLITE_VERIFICATION_LEASE_RELEASE_STORAGE_FUNCTION,
    { deterministic: true },
    verificationLeaseReleaseStorage,
  );
  database.function(
    NODE_SQLITE_VERIFICATION_FINALIZATION_DOCUMENT_STORAGE_FUNCTION,
    { deterministic: true },
    verificationFinalizationDocumentStorage,
  );
  database.function(
    NODE_SQLITE_VERIFICATION_FINALIZATION_PAYLOAD_MATCH_FUNCTION,
    { deterministic: true },
    verificationFinalizationPayloadMatch,
  );
  database.function(
    NODE_SQLITE_VERIFICATION_TERMINAL_PAYLOAD_PAIR_MATCH_FUNCTION,
    { deterministic: true },
    verificationTerminalPayloadPairMatch,
  );
  database.function(
    NODE_SQLITE_VERIFICATION_STAGE_PROJECTION_MATCH_FUNCTION,
    { deterministic: true },
    verificationStageProjectionMatch,
  );
  database.function(
    NODE_SQLITE_VERIFICATION_LEASE_PROJECTION_MATCH_FUNCTION,
    { deterministic: true },
    verificationLeaseProjectionMatch,
  );
  database.function(
    NODE_SQLITE_VERIFICATION_SOURCE_AUTHORITY_MATCH_FUNCTION,
    { deterministic: true },
    verificationSourceAuthorityMatch,
  );
  database.function(
    NODE_SQLITE_TASK_VERIFICATION_FINALIZATION_PAYLOAD_STORAGE_FUNCTION,
    { deterministic: true },
    taskVerificationFinalizationPayloadStorage,
  );
  database.function(
    NODE_SQLITE_TASK_VERIFICATION_FINALIZATION_DOCUMENT_STORAGE_FUNCTION,
    { deterministic: true },
    taskVerificationFinalizationDocumentStorage,
  );
  database.function(
    NODE_SQLITE_TASK_VERIFICATION_FINALIZATION_MARKER_MATCH_FUNCTION,
    { deterministic: true },
    taskVerificationFinalizationMarkerMatch,
  );
  database.function(
    NODE_SQLITE_TASK_VERIFICATION_FINALIZATION_PROJECTION_MATCH_FUNCTION,
    { deterministic: true },
    taskVerificationFinalizationProjectionMatch,
  );
  database.function(
    NODE_SQLITE_RUN_ONCE_CANONICAL_BLOB_MATCH_FUNCTION,
    { deterministic: true },
    runOnceCanonicalBlobMatch,
  );
  database.function(
    NODE_SQLITE_RUN_ONCE_ACTIVATION_IDENTITY_MATCH_FUNCTION,
    { deterministic: true },
    runOnceActivationIdentityMatch,
  );
  database.function(
    NODE_SQLITE_RUN_ONCE_STEP_IDENTITY_MATCH_FUNCTION,
    { deterministic: true },
    runOnceStepIdentityMatch,
  );
  database.function(
    NODE_SQLITE_RUN_ONCE_MARKER_MATCH_FUNCTION,
    { deterministic: true },
    runOnceMarkerMatch,
  );
  database.function(
    NODE_SQLITE_RUN_ONCE_SOURCE_FINGERPRINT_MATCH_FUNCTION,
    { deterministic: true },
    runOnceSourceFingerprintMatch,
  );
  database.function(
    NODE_SQLITE_RUN_ONCE_MODE_COMMAND_FINGERPRINT_MATCH_FUNCTION,
    { deterministic: true },
    runOnceModeCommandFingerprintMatch,
  );
  database.function(
    NODE_SQLITE_RUN_ONCE_MODE_EVENT_MATCH_FUNCTION,
    { deterministic: true },
    runOnceModeEventMatch,
  );
  // Explicit overloads keep legacy five/eight-argument guards strict. Only the
  // selected-task activation trigger supplies the separately bound task ID.
  database.function(
    NODE_SQLITE_RUN_ONCE_MODE_COMMAND_FINGERPRINT_MATCH_FUNCTION,
    { deterministic: true },
    (actual, commandId, projectId, expectedRevision, mode, taskId) =>
      runOnceModeCommandFingerprintMatch(
        actual,
        commandId,
        projectId,
        expectedRevision,
        mode,
        taskId,
      ),
  );
  database.function(
    NODE_SQLITE_RUN_ONCE_MODE_EVENT_MATCH_FUNCTION,
    { deterministic: true },
    (
      payload,
      metadata,
      projectId,
      previousMode,
      mode,
      previousPausedMode,
      pausedMode,
      changedAt,
      taskId,
    ) =>
      runOnceModeEventMatch(
        payload,
        metadata,
        projectId,
        previousMode,
        mode,
        previousPausedMode,
        pausedMode,
        changedAt,
        taskId,
      ),
  );
  database.function(
    NODE_SQLITE_RUN_ONCE_THREAD_ACTIVATION_IDENTITY_MATCH_FUNCTION,
    { deterministic: true },
    runOnceThreadActivationIdentityMatch,
  );
};

const ATTR_DB_SYSTEM_NAME = "db.system.name";

export const TypeId: TypeId = "~local/sqlite-node/SqliteClient";

export type TypeId = "~local/sqlite-node/SqliteClient";

type MaterializationCommitBoundary =
  | "open"
  | "orchestration"
  | "coordinator"
  | "prepare"
  | "initialPlanningFinalization"
  | "implementationAdmissionPending"
  | "implementationAdmission"
  | "implementationMaterializationPending"
  | "implementationMaterialization"
  | "implementationTurnAcceptance"
  | "implementationStageStartPending"
  | "implementationStageStart"
  | "implementationStageFinalizationPending"
  | "implementationStageFinalization"
  | "verificationAdmissionPending"
  | "verificationAdmission"
  | "verificationMaterializationPending"
  | "verificationMaterialization"
  | "verificationTurnAcceptance"
  | "verificationStageStartPending"
  | "verificationStageStart"
  | "verificationEvaluationEvidence"
  | "verificationEvaluationReceipt"
  | "verificationEvaluation"
  | "verificationStageFinalizationPending"
  | "verificationStageFinalization"
  | "taskVerificationFinalizationEvidence"
  | "taskVerificationFinalizationReceipt"
  | "taskVerificationFinalizationPublication"
  | "taskVerificationFinalization"
  | "runOnceStep";

interface MaterializationSavepointFrame {
  readonly name: string;
  readonly boundaryBeforeSavepoint: MaterializationCommitBoundary;
}

interface MaterializationStatementSnapshot {
  readonly wasInTransaction: boolean;
  readonly boundaryValid: boolean;
  readonly boundary: MaterializationCommitBoundary;
  readonly savepoints: ReadonlyArray<MaterializationSavepointFrame>;
}

type MaterializationStatement =
  | { readonly _tag: "none" }
  | { readonly _tag: "read" }
  | { readonly _tag: "begin" }
  | { readonly _tag: "commit" }
  | { readonly _tag: "rollback" }
  | { readonly _tag: "savepoint"; readonly name: string }
  | { readonly _tag: "rollbackTo"; readonly name: string }
  | { readonly _tag: "release"; readonly name: string }
  | {
      readonly _tag: "orchestrationMarker";
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "coordinatorMarker";
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "prepareMarker";
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "initialPlanningHandoff";
      readonly table: string;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "initialPlanningFinalization";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "implementationAdmission";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "implementationMaterialization";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "implementationTurnAcceptance";
      readonly table: string;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "implementationStageStart";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "implementationStageFinalization";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "verificationAdmission";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "verificationMaterialization";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "verificationTurnAcceptance";
      readonly table: string;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "verificationStageStart";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "verificationEvaluation";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "verificationStageFinalization";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "taskVerificationFinalization";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "runOnceStepMarker";
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "markerMutation";
      readonly table?: string;
      readonly target?: "unqualified" | "main";
    }
  | { readonly _tag: "potentialMarkerDml" };

interface SqlWordToken {
  readonly _tag: "word";
  readonly value: string;
}

interface SqlIdentifierToken {
  readonly _tag: "doubleQuotedIdentifier" | "backtickIdentifier" | "bracketIdentifier";
  readonly value: string;
}

interface SqlStringToken {
  readonly _tag: "string";
}

interface SqlParameterToken {
  readonly _tag: "parameter";
}

interface SqlPunctuationToken {
  readonly _tag: "dot" | "comma" | "openParenthesis" | "closeParenthesis" | "semicolon";
}

interface SqlOperatorToken {
  readonly _tag: "operator";
  readonly value: string;
}

type SqlToken =
  | SqlWordToken
  | SqlIdentifierToken
  | SqlStringToken
  | SqlParameterToken
  | SqlPunctuationToken
  | SqlOperatorToken;

const ORCHESTRATION_MARKER_TABLE = "orchestration_agent_control_thread_materialization_receipts";
const COORDINATOR_MARKER_TABLE = "agent_control_controlled_thread_materialization_accepted";
const PREPARE_STATE_TABLE = "agent_control_controlled_thread_prepare_finalizations";
const PREPARE_MARKER_TABLE = "agent_control_controlled_thread_prepare_final_commit_markers";
const INITIAL_PLANNING_HANDOFF_TABLES = new Set([
  "agent_control_initial_planning_handoff_intents",
  "agent_control_initial_planning_handoff_receipts",
  "agent_control_initial_planning_handoff_accepted",
  "agent_control_initial_planning_deliveries",
]);
const INITIAL_PLANNING_FINALIZATION_MARKER_TABLE =
  "agent_control_initial_planning_finalization_markers";
const INITIAL_PLANNING_FINALIZATION_TABLES = new Set([
  "agent_control_initial_planning_stage_started",
  "agent_control_initial_planning_result_evidence",
  "agent_control_initial_planning_finalization_receipts",
  INITIAL_PLANNING_FINALIZATION_MARKER_TABLE,
]);
const IMPLEMENTATION_ADMISSION_MARKER_TABLE = "agent_control_implementation_admission_markers";
const IMPLEMENTATION_ADMISSION_TABLES = new Set([
  "agent_control_implementation_admission_evidence",
  "agent_control_implementation_admission_receipts",
  IMPLEMENTATION_ADMISSION_MARKER_TABLE,
]);
const IMPLEMENTATION_MATERIALIZATION_MARKER_TABLE =
  "agent_control_implementation_materialization_markers";
const IMPLEMENTATION_MATERIALIZATION_TABLES = new Set([
  "agent_control_implementation_materialization_evidence",
  "agent_control_implementation_materialization_receipts",
  "agent_control_implementation_handoff_intents",
  "agent_control_implementation_handoff_receipts",
  "agent_control_implementation_handoff_accepted",
  "agent_control_implementation_deliveries",
  IMPLEMENTATION_MATERIALIZATION_MARKER_TABLE,
]);
const IMPLEMENTATION_TURN_ACCEPTANCE_TABLE = "agent_control_implementation_turn_accepted";
const IMPLEMENTATION_STAGE_START_MARKER_TABLE =
  "agent_control_implementation_stage_started_markers";
const IMPLEMENTATION_STAGE_START_TABLES = new Set([
  "agent_control_implementation_stage_started_evidence",
  "agent_control_implementation_stage_started_receipts",
  IMPLEMENTATION_STAGE_START_MARKER_TABLE,
]);
const IMPLEMENTATION_STAGE_FINALIZATION_MARKER_TABLE =
  "agent_control_implementation_stage_finalization_markers";
const IMPLEMENTATION_STAGE_FINALIZATION_TABLES = new Set([
  "agent_control_implementation_result_evidence",
  "agent_control_implementation_stage_finalization_receipts",
  IMPLEMENTATION_STAGE_FINALIZATION_MARKER_TABLE,
]);
const VERIFICATION_ADMISSION_MARKER_TABLE = "agent_control_verification_admission_markers";
const VERIFICATION_ADMISSION_TABLES = new Set([
  "agent_control_verification_admission_evidence",
  "agent_control_verification_admission_receipts",
  VERIFICATION_ADMISSION_MARKER_TABLE,
]);
const VERIFICATION_MATERIALIZATION_MARKER_TABLE =
  "agent_control_verification_materialization_markers";
const VERIFICATION_MATERIALIZATION_TABLES = new Set([
  "agent_control_verification_materialization_evidence",
  "agent_control_verification_materialization_receipts",
  "agent_control_verification_handoff_intents",
  "agent_control_verification_handoff_receipts",
  "agent_control_verification_handoff_accepted",
  "agent_control_verification_deliveries",
  VERIFICATION_MATERIALIZATION_MARKER_TABLE,
]);
const VERIFICATION_TURN_ACCEPTANCE_TABLE = "agent_control_verification_turn_accepted";
const VERIFICATION_STAGE_START_MARKER_TABLE = "agent_control_verification_stage_started_markers";
const VERIFICATION_STAGE_START_TABLES = new Set([
  "agent_control_verification_stage_started_evidence",
  "agent_control_verification_stage_started_receipts",
  VERIFICATION_STAGE_START_MARKER_TABLE,
]);
const VERIFICATION_EVALUATION_MARKER_TABLE = "agent_control_verification_evaluation_markers";
const VERIFICATION_EVALUATION_TABLES = new Set([
  "agent_control_verification_evaluation_evidence",
  "agent_control_verification_evaluation_receipts",
  VERIFICATION_EVALUATION_MARKER_TABLE,
]);
const VERIFICATION_STAGE_FINALIZATION_MARKER_TABLE =
  "agent_control_verification_finalization_markers";
const VERIFICATION_STAGE_FINALIZATION_TABLES = new Set([
  "agent_control_verification_finalization_evidence",
  "agent_control_verification_finalization_receipts",
  VERIFICATION_STAGE_FINALIZATION_MARKER_TABLE,
]);
const TASK_VERIFICATION_FINALIZATION_MARKER_TABLE =
  "agent_control_task_verification_finalization_markers";
const TASK_VERIFICATION_FINALIZATION_PUBLICATION_TABLE =
  "agent_control_task_verification_finalization_publications";
const TASK_VERIFICATION_FINALIZATION_TABLES = new Set([
  "agent_control_task_verification_finalization_evidence",
  "agent_control_task_verification_finalization_receipts",
  TASK_VERIFICATION_FINALIZATION_PUBLICATION_TABLE,
  TASK_VERIFICATION_FINALIZATION_MARKER_TABLE,
]);
const RUN_ONCE_STEP_MARKER_TABLE = "agent_control_run_once_step_markers";
const IMPLEMENTATION_TRANSACTIONAL_EVIDENCE_TABLES = new Set([
  "agent_control_implementation_session_evidence",
  "agent_control_implementation_delivery_attestations",
]);
const VERIFICATION_TRANSACTIONAL_EVIDENCE_TABLES = new Set([
  "agent_control_verification_session_evidence",
  "agent_control_verification_delivery_attestations",
]);
const INSERT_CONFLICT_ALGORITHMS = new Set(["ABORT", "FAIL", "IGNORE", "REPLACE", "ROLLBACK"]);
const MATERIALIZATION_MARKER_TRANSACTION_REQUIRED =
  "persistent materialization marker DML requires an active caller-controlled transaction";

const isSqlIdentifierStart = (character: string): boolean =>
  /[A-Za-z_]/.test(character) || character.charCodeAt(0) >= 0x80;

const isSqlIdentifierContinue = (character: string): boolean =>
  /[A-Za-z0-9_$]/.test(character) || character.charCodeAt(0) >= 0x80;

const lexFirstSqlStatement = (sql: string): ReadonlyArray<SqlToken> => {
  const tokens: Array<SqlToken> = [];
  let index = 0;

  const readString = () => {
    index += 1;
    while (index < sql.length) {
      if (sql[index] !== "'") {
        index += 1;
        continue;
      }
      if (sql[index + 1] === "'") {
        index += 2;
        continue;
      }
      index += 1;
      tokens.push({ _tag: "string" });
      return;
    }
    throw new Error("unterminated SQL string literal");
  };

  const readQuotedIdentifier = (
    quote: '"' | "`",
    tag: "doubleQuotedIdentifier" | "backtickIdentifier",
  ) => {
    let value = "";
    index += 1;
    while (index < sql.length) {
      const character = sql[index]!;
      if (character !== quote) {
        value += character;
        index += 1;
        continue;
      }
      if (sql[index + 1] === quote) {
        value += quote;
        index += 2;
        continue;
      }
      index += 1;
      tokens.push({ _tag: tag, value });
      return;
    }
    throw new Error("unterminated SQL quoted identifier");
  };

  while (index < sql.length) {
    const character = sql[index]!;
    if (/\s/.test(character)) {
      index += 1;
      continue;
    }
    if (character === "-" && sql[index + 1] === "-") {
      index += 2;
      while (index < sql.length && sql[index] !== "\n" && sql[index] !== "\r") {
        index += 1;
      }
      continue;
    }
    if (character === "/" && sql[index + 1] === "*") {
      const end = sql.indexOf("*/", index + 2);
      if (end < 0) {
        throw new Error("unterminated SQL block comment");
      }
      index = end + 2;
      continue;
    }
    if (character === "'") {
      readString();
      continue;
    }
    if (character === '"') {
      readQuotedIdentifier(character, "doubleQuotedIdentifier");
      continue;
    }
    if (character === "`") {
      readQuotedIdentifier(character, "backtickIdentifier");
      continue;
    }
    if (character === "[") {
      const end = sql.indexOf("]", index + 1);
      if (end < 0) {
        throw new Error("unterminated SQL bracket identifier");
      }
      const value = sql.slice(index + 1, end);
      index = end + 1;
      tokens.push({ _tag: "bracketIdentifier", value });
      continue;
    }
    if (character === ";") {
      if (tokens.length === 0) {
        index += 1;
        continue;
      }
      tokens.push({ _tag: "semicolon" });
      break;
    }
    if (character === ".") {
      tokens.push({ _tag: "dot" });
      index += 1;
      continue;
    }
    if (character === ",") {
      tokens.push({ _tag: "comma" });
      index += 1;
      continue;
    }
    if (character === "(") {
      tokens.push({ _tag: "openParenthesis" });
      index += 1;
      continue;
    }
    if (character === ")") {
      tokens.push({ _tag: "closeParenthesis" });
      index += 1;
      continue;
    }
    if (
      character === "?" ||
      ((character === ":" || character === "@" || character === "$") &&
        sql[index + 1] !== undefined &&
        isSqlIdentifierStart(sql[index + 1]!))
    ) {
      index += 1;
      while (index < sql.length && isSqlIdentifierContinue(sql[index]!)) {
        index += 1;
      }
      tokens.push({ _tag: "parameter" });
      continue;
    }
    if (isSqlIdentifierStart(character)) {
      const start = index;
      index += 1;
      while (index < sql.length && isSqlIdentifierContinue(sql[index]!)) {
        index += 1;
      }
      const value = sql.slice(start, index);
      tokens.push({ _tag: "word", value: value.toUpperCase() });
      continue;
    }
    tokens.push({ _tag: "operator", value: character });
    index += 1;
  }

  return tokens;
};

const isWord = (token: SqlToken | undefined, value?: string): token is SqlWordToken =>
  token?._tag === "word" && (value === undefined || token.value === value);

const isIdentifier = (token: SqlToken | undefined): token is SqlWordToken | SqlIdentifierToken =>
  token?._tag === "word" ||
  token?._tag === "doubleQuotedIdentifier" ||
  token?._tag === "backtickIdentifier" ||
  token?._tag === "bracketIdentifier";

const normalizedIdentifier = (token: SqlWordToken | SqlIdentifierToken): string =>
  token.value.toLowerCase();

const identifierName = (token: SqlToken | undefined): string | undefined =>
  isIdentifier(token) ? normalizedIdentifier(token) : undefined;

const skipParenthesizedTokens = (
  tokens: ReadonlyArray<SqlToken>,
  start: number,
): number | undefined => {
  if (tokens[start]?._tag !== "openParenthesis") {
    return undefined;
  }
  let depth = 0;
  for (let index = start; index < tokens.length; index += 1) {
    const token = tokens[index]!;
    if (token._tag === "openParenthesis") {
      depth += 1;
    } else if (token._tag === "closeParenthesis") {
      depth -= 1;
      if (depth === 0) {
        return index + 1;
      }
    } else if (token._tag === "semicolon") {
      return undefined;
    }
  }
  return undefined;
};

const parseWithPrefix = (tokens: ReadonlyArray<SqlToken>, start: number): number | undefined => {
  let index = start;
  if (!isWord(tokens[index], "WITH")) {
    return undefined;
  }
  index += 1;
  if (isWord(tokens[index], "RECURSIVE")) {
    index += 1;
  }

  while (index < tokens.length) {
    if (!isIdentifier(tokens[index])) {
      return undefined;
    }
    index += 1;

    if (tokens[index]?._tag === "openParenthesis") {
      const afterColumns = skipParenthesizedTokens(tokens, index);
      if (afterColumns === undefined) {
        return undefined;
      }
      index = afterColumns;
    }

    if (!isWord(tokens[index], "AS")) {
      return undefined;
    }
    index += 1;
    if (isWord(tokens[index], "MATERIALIZED")) {
      index += 1;
    } else if (isWord(tokens[index], "NOT") && isWord(tokens[index + 1], "MATERIALIZED")) {
      index += 2;
    }

    const afterBody = skipParenthesizedTokens(tokens, index);
    if (afterBody === undefined) {
      return undefined;
    }
    index = afterBody;
    if (tokens[index]?._tag !== "comma") {
      return index;
    }
    index += 1;
  }
  return undefined;
};

const parseInsertTarget = (
  tokens: ReadonlyArray<SqlToken>,
  start: number,
): MaterializationStatement => {
  let index = start;
  if (isWord(tokens[index], "INSERT")) {
    index += 1;
    if (isWord(tokens[index], "OR")) {
      const conflictAlgorithm = tokens[index + 1];
      if (!isWord(conflictAlgorithm) || !INSERT_CONFLICT_ALGORITHMS.has(conflictAlgorithm.value)) {
        return { _tag: "potentialMarkerDml" };
      }
      index += 2;
    }
  } else if (isWord(tokens[index], "REPLACE")) {
    index += 1;
  } else {
    return { _tag: "none" };
  }

  if (!isWord(tokens[index], "INTO")) {
    return { _tag: "potentialMarkerDml" };
  }
  index += 1;

  const firstIdentifier = tokens[index];
  if (!isIdentifier(firstIdentifier)) {
    return { _tag: "potentialMarkerDml" };
  }
  let schema: string | undefined;
  let table = normalizedIdentifier(firstIdentifier);
  index += 1;

  if (tokens[index]?._tag === "dot") {
    const tableIdentifier = tokens[index + 1];
    if (!isIdentifier(tableIdentifier)) {
      return { _tag: "potentialMarkerDml" };
    }
    schema = table;
    table = normalizedIdentifier(tableIdentifier);
    index += 2;
    if (tokens[index]?._tag === "dot") {
      return { _tag: "potentialMarkerDml" };
    }
  }

  if (schema !== undefined && schema !== "main") {
    return { _tag: "none" };
  }
  if (table === ORCHESTRATION_MARKER_TABLE) {
    return { _tag: "orchestrationMarker", target: schema === "main" ? "main" : "unqualified" };
  }
  if (table === COORDINATOR_MARKER_TABLE) {
    return { _tag: "coordinatorMarker", target: schema === "main" ? "main" : "unqualified" };
  }
  if (table === PREPARE_MARKER_TABLE) {
    return { _tag: "prepareMarker", target: schema === "main" ? "main" : "unqualified" };
  }
  if (INITIAL_PLANNING_HANDOFF_TABLES.has(table)) {
    return {
      _tag: "initialPlanningHandoff",
      table,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (INITIAL_PLANNING_FINALIZATION_TABLES.has(table)) {
    return {
      _tag: "initialPlanningFinalization",
      table,
      final: table === INITIAL_PLANNING_FINALIZATION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (IMPLEMENTATION_ADMISSION_TABLES.has(table)) {
    return {
      _tag: "implementationAdmission",
      table,
      final: table === IMPLEMENTATION_ADMISSION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (IMPLEMENTATION_MATERIALIZATION_TABLES.has(table)) {
    return {
      _tag: "implementationMaterialization",
      table,
      final: table === IMPLEMENTATION_MATERIALIZATION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (table === IMPLEMENTATION_TURN_ACCEPTANCE_TABLE) {
    return {
      _tag: "implementationTurnAcceptance",
      table,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (IMPLEMENTATION_STAGE_START_TABLES.has(table)) {
    return {
      _tag: "implementationStageStart",
      table,
      final: table === IMPLEMENTATION_STAGE_START_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (IMPLEMENTATION_STAGE_FINALIZATION_TABLES.has(table)) {
    return {
      _tag: "implementationStageFinalization",
      table,
      final: table === IMPLEMENTATION_STAGE_FINALIZATION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (VERIFICATION_ADMISSION_TABLES.has(table)) {
    return {
      _tag: "verificationAdmission",
      table,
      final: table === VERIFICATION_ADMISSION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (VERIFICATION_MATERIALIZATION_TABLES.has(table)) {
    return {
      _tag: "verificationMaterialization",
      table,
      final: table === VERIFICATION_MATERIALIZATION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (table === VERIFICATION_TURN_ACCEPTANCE_TABLE) {
    return {
      _tag: "verificationTurnAcceptance",
      table,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (VERIFICATION_STAGE_START_TABLES.has(table)) {
    return {
      _tag: "verificationStageStart",
      table,
      final: table === VERIFICATION_STAGE_START_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (VERIFICATION_EVALUATION_TABLES.has(table)) {
    return {
      _tag: "verificationEvaluation",
      table,
      final: table === VERIFICATION_EVALUATION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (VERIFICATION_STAGE_FINALIZATION_TABLES.has(table)) {
    return {
      _tag: "verificationStageFinalization",
      table,
      final: table === VERIFICATION_STAGE_FINALIZATION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (TASK_VERIFICATION_FINALIZATION_TABLES.has(table)) {
    return {
      _tag: "taskVerificationFinalization",
      table,
      final: table === TASK_VERIFICATION_FINALIZATION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (table === RUN_ONCE_STEP_MARKER_TABLE) {
    return {
      _tag: "runOnceStepMarker",
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (
    IMPLEMENTATION_TRANSACTIONAL_EVIDENCE_TABLES.has(table) ||
    VERIFICATION_TRANSACTIONAL_EVIDENCE_TABLES.has(table)
  ) {
    return {
      _tag: "markerMutation",
      table,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (table === PREPARE_STATE_TABLE) {
    return {
      _tag: "markerMutation",
      table,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  return { _tag: "none" };
};

const parseUpdateOrDeleteTarget = (
  tokens: ReadonlyArray<SqlToken>,
  start: number,
): MaterializationStatement => {
  let index = start;
  if (isWord(tokens[index], "UPDATE")) {
    index += 1;
    if (isWord(tokens[index], "OR")) {
      const conflictAlgorithm = tokens[index + 1];
      if (!isWord(conflictAlgorithm) || !INSERT_CONFLICT_ALGORITHMS.has(conflictAlgorithm.value)) {
        return { _tag: "potentialMarkerDml" };
      }
      index += 2;
    }
  } else if (isWord(tokens[index], "DELETE")) {
    index += 1;
    if (!isWord(tokens[index], "FROM")) {
      return { _tag: "potentialMarkerDml" };
    }
    index += 1;
  } else {
    return { _tag: "none" };
  }

  const firstIdentifier = tokens[index];
  if (!isIdentifier(firstIdentifier)) {
    return { _tag: "potentialMarkerDml" };
  }
  let schema: string | undefined;
  let table = normalizedIdentifier(firstIdentifier);
  index += 1;
  if (tokens[index]?._tag === "dot") {
    const tableIdentifier = tokens[index + 1];
    if (!isIdentifier(tableIdentifier)) {
      return { _tag: "potentialMarkerDml" };
    }
    schema = table;
    table = normalizedIdentifier(tableIdentifier);
  }

  if (schema !== undefined && schema !== "main") {
    return { _tag: "none" };
  }
  if (table === TASK_VERIFICATION_FINALIZATION_PUBLICATION_TABLE) {
    return { _tag: "none" };
  }
  return table === ORCHESTRATION_MARKER_TABLE ||
    table === COORDINATOR_MARKER_TABLE ||
    table === PREPARE_STATE_TABLE ||
    table === PREPARE_MARKER_TABLE ||
    INITIAL_PLANNING_FINALIZATION_TABLES.has(table) ||
    IMPLEMENTATION_ADMISSION_TABLES.has(table) ||
    IMPLEMENTATION_MATERIALIZATION_TABLES.has(table) ||
    table === IMPLEMENTATION_TURN_ACCEPTANCE_TABLE ||
    IMPLEMENTATION_STAGE_START_TABLES.has(table) ||
    IMPLEMENTATION_STAGE_FINALIZATION_TABLES.has(table) ||
    VERIFICATION_ADMISSION_TABLES.has(table) ||
    VERIFICATION_MATERIALIZATION_TABLES.has(table) ||
    table === VERIFICATION_TURN_ACCEPTANCE_TABLE ||
    VERIFICATION_STAGE_START_TABLES.has(table) ||
    VERIFICATION_EVALUATION_TABLES.has(table) ||
    VERIFICATION_STAGE_FINALIZATION_TABLES.has(table) ||
    TASK_VERIFICATION_FINALIZATION_TABLES.has(table) ||
    IMPLEMENTATION_TRANSACTIONAL_EVIDENCE_TABLES.has(table) ||
    VERIFICATION_TRANSACTIONAL_EVIDENCE_TABLES.has(table)
    ? {
        _tag: "markerMutation",
        table,
        target: schema === "main" ? "main" : "unqualified",
      }
    : { _tag: "none" };
};

const parseMaterializationStatement = (sql: string): MaterializationStatement => {
  let tokens: ReadonlyArray<SqlToken>;
  try {
    tokens = lexFirstSqlStatement(sql);
  } catch {
    return { _tag: "none" };
  }
  const semicolonIndex = tokens.findIndex((token) => token._tag === "semicolon");
  if (semicolonIndex >= 0) {
    tokens = tokens.slice(0, semicolonIndex);
  }

  const first = tokens[0];
  if (isWord(first, "INSERT") || isWord(first, "REPLACE")) {
    return parseInsertTarget(tokens, 0);
  }
  if (isWord(first, "UPDATE") || isWord(first, "DELETE")) {
    return parseUpdateOrDeleteTarget(tokens, 0);
  }
  if (isWord(first, "SELECT") || isWord(first, "VALUES")) {
    return { _tag: "read" };
  }
  if (isWord(first, "WITH")) {
    const statementStart = parseWithPrefix(tokens, 0);
    if (statementStart === undefined) {
      return { _tag: "potentialMarkerDml" };
    }
    if (isWord(tokens[statementStart], "INSERT") || isWord(tokens[statementStart], "REPLACE")) {
      return parseInsertTarget(tokens, statementStart);
    }
    if (isWord(tokens[statementStart], "UPDATE") || isWord(tokens[statementStart], "DELETE")) {
      return parseUpdateOrDeleteTarget(tokens, statementStart);
    }
    return isWord(tokens[statementStart], "SELECT") || isWord(tokens[statementStart], "VALUES")
      ? { _tag: "read" }
      : { _tag: "potentialMarkerDml" };
  }

  if (isWord(first, "BEGIN")) {
    let index = 1;
    if (
      isWord(tokens[index], "DEFERRED") ||
      isWord(tokens[index], "IMMEDIATE") ||
      isWord(tokens[index], "EXCLUSIVE")
    ) {
      index += 1;
    }
    if (isWord(tokens[index], "TRANSACTION")) {
      index += 1;
    }
    return index === tokens.length ? { _tag: "begin" } : { _tag: "none" };
  }
  if (isWord(first, "COMMIT") || isWord(first, "END")) {
    return tokens.length === 1 || (tokens.length === 2 && isWord(tokens[1], "TRANSACTION"))
      ? { _tag: "commit" }
      : { _tag: "none" };
  }
  if (isWord(first, "ROLLBACK")) {
    let index = 1;
    if (isWord(tokens[index], "TRANSACTION")) {
      index += 1;
    }
    if (index === tokens.length) {
      return { _tag: "rollback" };
    }
    if (!isWord(tokens[index], "TO")) {
      return { _tag: "none" };
    }
    index += 1;
    if (isWord(tokens[index], "SAVEPOINT")) {
      index += 1;
    }
    const name = identifierName(tokens[index]);
    return name !== undefined && index + 1 === tokens.length
      ? { _tag: "rollbackTo", name }
      : { _tag: "none" };
  }
  if (tokens.length === 2 && isWord(first, "SAVEPOINT")) {
    const name = identifierName(tokens[1]);
    return name === undefined ? { _tag: "none" } : { _tag: "savepoint", name };
  }
  if (isWord(first, "RELEASE")) {
    let index = 1;
    if (isWord(tokens[index], "SAVEPOINT")) {
      index += 1;
    }
    const name = identifierName(tokens[index]);
    return name !== undefined && index + 1 === tokens.length
      ? { _tag: "release", name }
      : { _tag: "none" };
  }

  return { _tag: "none" };
};

export interface SqliteClientConfig {
  readonly filename: string;
  readonly readonly?: boolean | undefined;
  readonly allowExtension?: boolean | undefined;
  readonly prepareCacheSize?: number | undefined;
  readonly prepareCacheTTL?: Duration.Input | undefined;
  readonly spanAttributes?: Record<string, unknown> | undefined;
  readonly transformResultNames?: ((str: string) => string) | undefined;
  readonly transformQueryNames?: ((str: string) => string) | undefined;
}

interface SqliteClientInternalConfig extends SqliteClientConfig {
  /** @internal Test-only fault injection for synchronous marker change-count reads. */
  readonly _testHooks?: {
    readonly beforeMarkerChanges?: (() => void) | undefined;
    readonly registerFunctions?: ((database: NodeSqlite.DatabaseSync) => void) | undefined;
  };
}

export interface SqliteMemoryClientConfig extends Omit<
  SqliteClientConfig,
  "filename" | "readonly"
> {
  /** @internal Test-only fault injection for synchronous marker change-count reads. */
  readonly _testHooks?: {
    readonly beforeMarkerChanges?: (() => void) | undefined;
    readonly registerFunctions?: ((database: NodeSqlite.DatabaseSync) => void) | undefined;
  };
}

export class UnsupportedNodeSqliteVersionError extends Schema.TaggedError<UnsupportedNodeSqliteVersionError>()(
  "UnsupportedNodeSqliteVersionError",
  {
    nodeVersion: Schema.String,
    requirement: Schema.String,
  },
) {
  override get message(): string {
    return `Node.js ${this.nodeVersion} is missing required node:sqlite APIs. Upgrade to ${this.requirement}.`;
  }
}

export class UnsupportedNodeSqliteOperationError extends Schema.TaggedError<UnsupportedNodeSqliteOperationError>()(
  "UnsupportedNodeSqliteOperationError",
  {},
) {
  override get message(): string {
    return "Node SQLite does not support executeStream.";
  }
}

/**
 * Verify that the current Node.js version includes the `node:sqlite` APIs
 * used by `NodeSqliteClient` — specifically `StatementSync.columns()` (added
 * in Node 22.16.0 / 23.11.0).
 *
 * @see https://github.com/nodejs/node/pull/57490
 */
const checkNodeSqliteCompat = () => {
  const parts = process.versions.node.split(".").map(Number);
  const major = parts[0] ?? 0;
  const minor = parts[1] ?? 0;
  const supported = (major === 22 && minor >= 16) || (major === 23 && minor >= 11) || major >= 24;

  if (!supported) {
    return Effect.die(
      new UnsupportedNodeSqliteVersionError({
        nodeVersion: process.versions.node,
        requirement: "Node.js >=22.16, >=23.11, or >=24",
      }),
    );
  }
  return Effect.void;
};

const makeWithDatabase = Effect.fn("makeWithDatabase")(function* (
  options: SqliteClientInternalConfig,
  openDatabase: () => NodeSqlite.DatabaseSync,
): Effect.fn.Return<Client.SqlClient, SqlError, Scope.Scope | Reactivity.Reactivity> {
  yield* checkNodeSqliteCompat();

  const compiler = Statement.makeCompilerSqlite(options.transformQueryNames);
  const transformRows = options.transformResultNames
    ? Statement.defaultTransforms(options.transformResultNames).array
    : undefined;

  const makeConnection = Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const db = yield* Effect.try({
      try: () => {
        const database = openDatabase();
        try {
          (options._testHooks?.registerFunctions ?? registerNodeSqliteFunctions)(database);
          return database;
        } catch (cause) {
          database.close();
          throw cause;
        }
      },
      catch: (cause) =>
        new SqlError({
          reason: classifySqliteError(cause, {
            message: "Failed to open database",
            operation: "open",
          }),
        }),
    });
    yield* Scope.addFinalizer(
      scope,
      Effect.try({
        try: () => db.close(),
        catch: (cause) =>
          new SqlError({
            reason: classifySqliteError(cause, {
              message: "Failed to close database",
              operation: "close",
            }),
          }),
      }).pipe(Effect.orDie),
    );

    const statementReaderCache = new WeakMap<NodeSqlite.StatementSync, boolean>();
    let materializationCommitBoundary: MaterializationCommitBoundary = "open";
    const materializationSavepoints: Array<MaterializationSavepointFrame> = [];
    let materializationBoundaryValid = true;
    const resetMaterializationCommitState = () => {
      materializationCommitBoundary = "open";
      materializationSavepoints.length = 0;
      materializationBoundaryValid = true;
    };
    const findMaterializationSavepoint = (name: string): number => {
      for (let index = materializationSavepoints.length - 1; index >= 0; index -= 1) {
        if (materializationSavepoints[index]?.name === name) {
          return index;
        }
      }
      return -1;
    };
    const snapshotMaterializationStatement = (): MaterializationStatementSnapshot => ({
      wasInTransaction: db.isTransaction,
      boundaryValid: materializationBoundaryValid,
      boundary: materializationCommitBoundary,
      savepoints: materializationSavepoints.slice(),
    });
    const requireMaterializationMarkerTransaction = (
      statement: MaterializationStatement,
      snapshot: MaterializationStatementSnapshot,
    ) => {
      if (
        snapshot.wasInTransaction ||
        (statement._tag !== "orchestrationMarker" &&
          statement._tag !== "coordinatorMarker" &&
          statement._tag !== "prepareMarker" &&
          statement._tag !== "initialPlanningHandoff" &&
          statement._tag !== "initialPlanningFinalization" &&
          statement._tag !== "implementationAdmission" &&
          statement._tag !== "implementationMaterialization" &&
          statement._tag !== "implementationTurnAcceptance" &&
          statement._tag !== "implementationStageStart" &&
          statement._tag !== "implementationStageFinalization" &&
          statement._tag !== "verificationAdmission" &&
          statement._tag !== "verificationMaterialization" &&
          statement._tag !== "verificationTurnAcceptance" &&
          statement._tag !== "verificationStageStart" &&
          statement._tag !== "verificationEvaluation" &&
          statement._tag !== "verificationStageFinalization" &&
          statement._tag !== "taskVerificationFinalization" &&
          statement._tag !== "runOnceStepMarker" &&
          statement._tag !== "markerMutation")
      ) {
        return;
      }
      resetMaterializationCommitState();
      throw new Error(MATERIALIZATION_MARKER_TRANSACTION_REQUIRED);
    };
    const ensureMaterializationCommitBoundary = (
      statement: MaterializationStatement,
      snapshot: MaterializationStatementSnapshot,
    ) => {
      if (statement._tag === "potentialMarkerDml") {
        if (snapshot.wasInTransaction) {
          materializationBoundaryValid = false;
        }
        throw new Error("potential materialization marker DML could not be classified safely");
      }
      if (!snapshot.wasInTransaction) {
        return;
      }
      if (!snapshot.boundaryValid) {
        if (statement._tag === "rollback") {
          return;
        }
        throw new Error(
          "controlled thread materialization boundary is invalid after transaction-control failure",
        );
      }
      if (statement._tag === "read") {
        return;
      }
      if (
        statement._tag === "begin" ||
        statement._tag === "commit" ||
        statement._tag === "rollback" ||
        statement._tag === "savepoint" ||
        statement._tag === "rollbackTo" ||
        statement._tag === "release"
      ) {
        if (
          (snapshot.boundary === "implementationAdmissionPending" ||
            snapshot.boundary === "implementationMaterializationPending" ||
            snapshot.boundary === "implementationStageStartPending" ||
            snapshot.boundary === "implementationStageFinalizationPending" ||
            snapshot.boundary === "verificationAdmissionPending" ||
            snapshot.boundary === "verificationMaterializationPending" ||
            snapshot.boundary === "verificationStageStartPending" ||
            snapshot.boundary === "verificationEvaluationEvidence" ||
            snapshot.boundary === "verificationEvaluationReceipt" ||
            snapshot.boundary === "verificationStageFinalizationPending" ||
            snapshot.boundary === "taskVerificationFinalizationEvidence" ||
            snapshot.boundary === "taskVerificationFinalizationReceipt" ||
            snapshot.boundary === "taskVerificationFinalizationPublication") &&
          (statement._tag === "commit" ||
            (statement._tag === "release" &&
              snapshot.savepoints.length === 1 &&
              snapshot.savepoints[0]?.name === statement.name))
        ) {
          materializationBoundaryValid = false;
          throw new Error(
            snapshot.boundary === "implementationAdmissionPending"
              ? "implementation admission companion chain requires a final marker"
              : snapshot.boundary === "verificationAdmissionPending"
                ? "verification admission companion chain requires a final marker"
                : snapshot.boundary === "verificationMaterializationPending" ||
                    snapshot.boundary === "verificationStageStartPending"
                  ? "verification companion chain requires a final marker"
                  : snapshot.boundary === "verificationEvaluationEvidence" ||
                      snapshot.boundary === "verificationEvaluationReceipt"
                    ? "verification evaluation companion chain requires a final marker"
                    : snapshot.boundary === "verificationStageFinalizationPending"
                      ? "verification finalization companion chain requires a final marker"
                      : snapshot.boundary === "taskVerificationFinalizationEvidence" ||
                          snapshot.boundary === "taskVerificationFinalizationReceipt" ||
                          snapshot.boundary === "taskVerificationFinalizationPublication"
                        ? "task Verification finalization companion chain requires a final marker"
                        : "implementation companion chain requires a final marker",
          );
        }
        return;
      }
      const coordinatorHandoff =
        snapshot.boundary === "orchestration" && statement._tag === "coordinatorMarker";
      const initialPlanningHandoff =
        snapshot.boundary === "orchestration" && statement._tag === "initialPlanningHandoff";
      const implementationAdmissionCompanion =
        snapshot.boundary === "implementationAdmissionPending" &&
        statement._tag === "implementationAdmission";
      const implementationMaterializationCompanion =
        (snapshot.boundary === "orchestration" ||
          snapshot.boundary === "implementationMaterializationPending") &&
        statement._tag === "implementationMaterialization";
      const implementationStageStartCompanion =
        snapshot.boundary === "implementationStageStartPending" &&
        statement._tag === "implementationStageStart";
      const implementationStageFinalizationCompanion =
        snapshot.boundary === "implementationStageFinalizationPending" &&
        statement._tag === "implementationStageFinalization";
      const verificationAdmissionCompanion =
        snapshot.boundary === "verificationAdmissionPending" &&
        statement._tag === "verificationAdmission";
      const verificationMaterializationCompanion =
        (snapshot.boundary === "orchestration" ||
          snapshot.boundary === "verificationMaterializationPending") &&
        statement._tag === "verificationMaterialization";
      const verificationStageStartCompanion =
        snapshot.boundary === "verificationStageStartPending" &&
        statement._tag === "verificationStageStart";
      const verificationEvaluationCompanion =
        (snapshot.boundary === "verificationEvaluationEvidence" ||
          snapshot.boundary === "verificationEvaluationReceipt") &&
        statement._tag === "verificationEvaluation";
      const verificationStageFinalizationCompanion =
        snapshot.boundary === "verificationStageFinalizationPending" &&
        statement._tag === "verificationStageFinalization";
      const taskVerificationFinalizationCompanion =
        (snapshot.boundary === "taskVerificationFinalizationEvidence" ||
          snapshot.boundary === "taskVerificationFinalizationReceipt" ||
          snapshot.boundary === "taskVerificationFinalizationPublication") &&
        statement._tag === "taskVerificationFinalization";
      if (
        snapshot.boundary !== "open" &&
        !coordinatorHandoff &&
        !initialPlanningHandoff &&
        !implementationAdmissionCompanion &&
        !implementationMaterializationCompanion &&
        !implementationStageStartCompanion &&
        !implementationStageFinalizationCompanion &&
        !verificationAdmissionCompanion &&
        !verificationMaterializationCompanion &&
        !verificationStageStartCompanion &&
        !verificationEvaluationCompanion &&
        !verificationStageFinalizationCompanion &&
        !taskVerificationFinalizationCompanion
      ) {
        materializationBoundaryValid = false;
        throw new Error(
          "controlled thread materialization marker must be the final transaction statement",
        );
      }
    };
    const updateMaterializationCommitBoundary = (
      statement: MaterializationStatement,
      snapshot: MaterializationStatementSnapshot,
      markerWriteChangedRows: boolean,
    ): boolean => {
      const transactionEnded = snapshot.wasInTransaction && !db.isTransaction;
      const committed =
        transactionEnded && (statement._tag === "commit" || statement._tag === "release");
      if (transactionEnded) {
        resetMaterializationCommitState();
        return (
          committed &&
          snapshot.boundaryValid &&
          (snapshot.boundary === "coordinator" ||
            snapshot.boundary === "prepare" ||
            snapshot.boundary === "initialPlanningFinalization" ||
            snapshot.boundary === "implementationAdmission" ||
            snapshot.boundary === "implementationMaterialization" ||
            snapshot.boundary === "implementationTurnAcceptance" ||
            snapshot.boundary === "implementationStageStart" ||
            snapshot.boundary === "implementationStageFinalization" ||
            snapshot.boundary === "verificationAdmission" ||
            snapshot.boundary === "verificationMaterialization" ||
            snapshot.boundary === "verificationTurnAcceptance" ||
            snapshot.boundary === "verificationStageStart" ||
            snapshot.boundary === "verificationEvaluation" ||
            snapshot.boundary === "verificationStageFinalization" ||
            snapshot.boundary === "taskVerificationFinalization" ||
            snapshot.boundary === "runOnceStep")
        );
      }

      const effectiveStatement =
        !markerWriteChangedRows &&
        (statement._tag === "orchestrationMarker" ||
          statement._tag === "coordinatorMarker" ||
          statement._tag === "prepareMarker" ||
          (statement._tag === "initialPlanningFinalization" && statement.final) ||
          (statement._tag === "implementationAdmission" && statement.final) ||
          (statement._tag === "implementationMaterialization" && statement.final) ||
          statement._tag === "implementationTurnAcceptance" ||
          (statement._tag === "implementationStageStart" && statement.final) ||
          (statement._tag === "implementationStageFinalization" && statement.final) ||
          (statement._tag === "verificationAdmission" && statement.final) ||
          (statement._tag === "verificationMaterialization" && statement.final) ||
          statement._tag === "verificationTurnAcceptance" ||
          (statement._tag === "verificationStageStart" && statement.final) ||
          (statement._tag === "verificationEvaluation" && statement.final) ||
          (statement._tag === "verificationStageFinalization" && statement.final) ||
          (statement._tag === "taskVerificationFinalization" && statement.final) ||
          statement._tag === "runOnceStepMarker")
          ? ({ _tag: "none" } as const)
          : statement;
      switch (effectiveStatement._tag) {
        case "begin": {
          resetMaterializationCommitState();
          return false;
        }
        case "savepoint": {
          materializationSavepoints.push({
            name: effectiveStatement.name,
            boundaryBeforeSavepoint: snapshot.boundary,
          });
          return false;
        }
        case "rollbackTo": {
          const savepointIndex = findMaterializationSavepoint(effectiveStatement.name);
          if (savepointIndex < 0) {
            materializationBoundaryValid = false;
            throw new Error(
              `untracked materialization savepoint rollback: ${effectiveStatement.name}`,
            );
          }
          materializationCommitBoundary =
            materializationSavepoints[savepointIndex]!.boundaryBeforeSavepoint;
          materializationSavepoints.length = savepointIndex + 1;
          return false;
        }
        case "release": {
          const savepointIndex = findMaterializationSavepoint(effectiveStatement.name);
          if (savepointIndex < 0) {
            materializationBoundaryValid = false;
            throw new Error(
              `untracked materialization savepoint release: ${effectiveStatement.name}`,
            );
          }
          materializationSavepoints.length = savepointIndex;
          return false;
        }
        case "commit":
        case "rollback": {
          resetMaterializationCommitState();
          return false;
        }
        case "potentialMarkerDml": {
          materializationBoundaryValid = false;
          return false;
        }
        case "orchestrationMarker": {
          if (db.isTransaction) {
            materializationCommitBoundary = "orchestration";
          }
          break;
        }
        case "coordinatorMarker": {
          if (db.isTransaction) {
            materializationCommitBoundary = "coordinator";
          }
          break;
        }
        case "prepareMarker": {
          if (db.isTransaction) {
            materializationCommitBoundary = "prepare";
          }
          break;
        }
        case "initialPlanningFinalization": {
          if (effectiveStatement.final && db.isTransaction) {
            materializationCommitBoundary = "initialPlanningFinalization";
          }
          break;
        }
        case "implementationAdmission": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "implementationAdmission"
              : "implementationAdmissionPending";
          }
          break;
        }
        case "implementationMaterialization": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "implementationMaterialization"
              : "implementationMaterializationPending";
          }
          break;
        }
        case "implementationTurnAcceptance": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = "implementationTurnAcceptance";
          }
          break;
        }
        case "implementationStageStart": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "implementationStageStart"
              : "implementationStageStartPending";
          }
          break;
        }
        case "implementationStageFinalization": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "implementationStageFinalization"
              : "implementationStageFinalizationPending";
          }
          break;
        }
        case "verificationAdmission": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "verificationAdmission"
              : "verificationAdmissionPending";
          }
          break;
        }
        case "verificationMaterialization": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "verificationMaterialization"
              : "verificationMaterializationPending";
          }
          break;
        }
        case "verificationTurnAcceptance": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = "verificationTurnAcceptance";
          }
          break;
        }
        case "verificationStageStart": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "verificationStageStart"
              : "verificationStageStartPending";
          }
          break;
        }
        case "verificationEvaluation": {
          if (markerWriteChangedRows && db.isTransaction) {
            if (
              snapshot.boundary === "open" &&
              effectiveStatement.table === "agent_control_verification_evaluation_evidence"
            ) {
              materializationCommitBoundary = "verificationEvaluationEvidence";
            } else if (
              snapshot.boundary === "verificationEvaluationEvidence" &&
              effectiveStatement.table === "agent_control_verification_evaluation_receipts"
            ) {
              materializationCommitBoundary = "verificationEvaluationReceipt";
            } else if (
              snapshot.boundary === "verificationEvaluationReceipt" &&
              effectiveStatement.final
            ) {
              materializationCommitBoundary = "verificationEvaluation";
            } else {
              materializationBoundaryValid = false;
              throw new Error("verification evaluation requires Evidence then Receipt then Marker");
            }
          }
          break;
        }
        case "verificationStageFinalization": {
          if (markerWriteChangedRows && db.isTransaction) {
            if (
              snapshot.boundary === "open" &&
              effectiveStatement.table === "agent_control_verification_finalization_evidence"
            ) {
              materializationCommitBoundary = "verificationStageFinalizationPending";
            } else if (
              snapshot.boundary === "verificationStageFinalizationPending" &&
              effectiveStatement.table === "agent_control_verification_finalization_receipts"
            ) {
              materializationCommitBoundary = "verificationStageFinalizationPending";
            } else if (
              snapshot.boundary === "verificationStageFinalizationPending" &&
              effectiveStatement.final
            ) {
              materializationCommitBoundary = "verificationStageFinalization";
            } else {
              materializationBoundaryValid = false;
              throw new Error(
                "verification finalization requires Evidence then Receipt then Marker",
              );
            }
          }
          break;
        }
        case "taskVerificationFinalization": {
          if (markerWriteChangedRows && db.isTransaction) {
            if (
              snapshot.boundary === "open" &&
              effectiveStatement.table === "agent_control_task_verification_finalization_evidence"
            ) {
              materializationCommitBoundary = "taskVerificationFinalizationEvidence";
            } else if (
              snapshot.boundary === "taskVerificationFinalizationEvidence" &&
              effectiveStatement.table === "agent_control_task_verification_finalization_receipts"
            ) {
              materializationCommitBoundary = "taskVerificationFinalizationReceipt";
            } else if (
              snapshot.boundary === "taskVerificationFinalizationReceipt" &&
              effectiveStatement.table === TASK_VERIFICATION_FINALIZATION_PUBLICATION_TABLE
            ) {
              materializationCommitBoundary = "taskVerificationFinalizationPublication";
            } else if (
              snapshot.boundary === "taskVerificationFinalizationPublication" &&
              effectiveStatement.final
            ) {
              materializationCommitBoundary = "taskVerificationFinalization";
            } else {
              materializationBoundaryValid = false;
              throw new Error(
                "task Verification finalization requires Evidence then Receipt then Publication then Marker",
              );
            }
          }
          break;
        }
        case "runOnceStepMarker": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = "runOnceStep";
          }
          break;
        }
        case "markerMutation":
        case "initialPlanningHandoff":
        case "read":
        case "none":
          break;
      }
      if (!db.isTransaction) {
        resetMaterializationCommitState();
      }
      return false;
    };
    const handleMaterializationStatementFailure = (statement: MaterializationStatement) => {
      if (
        db.isTransaction &&
        (statement._tag === "savepoint" ||
          statement._tag === "rollbackTo" ||
          statement._tag === "release" ||
          statement._tag === "orchestrationMarker" ||
          statement._tag === "coordinatorMarker" ||
          statement._tag === "prepareMarker" ||
          statement._tag === "initialPlanningHandoff" ||
          statement._tag === "initialPlanningFinalization" ||
          statement._tag === "implementationAdmission" ||
          statement._tag === "implementationMaterialization" ||
          statement._tag === "implementationTurnAcceptance" ||
          statement._tag === "implementationStageStart" ||
          statement._tag === "implementationStageFinalization" ||
          statement._tag === "verificationAdmission" ||
          statement._tag === "verificationMaterialization" ||
          statement._tag === "verificationTurnAcceptance" ||
          statement._tag === "verificationStageStart" ||
          statement._tag === "verificationEvaluation" ||
          statement._tag === "verificationStageFinalization" ||
          statement._tag === "taskVerificationFinalization" ||
          statement._tag === "runOnceStepMarker" ||
          statement._tag === "markerMutation" ||
          statement._tag === "potentialMarkerDml")
      ) {
        materializationBoundaryValid = false;
      }
      if (statement._tag === "commit") {
        if (db.isTransaction) {
          try {
            db.exec("ROLLBACK");
          } catch {
            // Preserve the original commit failure. The connection will
            // remain unavailable until its owning scope is closed.
          }
        }
        resetMaterializationCommitState();
        if (db.isTransaction) {
          materializationBoundaryValid = false;
        }
      } else if (statement._tag === "rollback") {
        if (db.isTransaction) {
          try {
            db.exec("ROLLBACK");
          } catch {
            // Preserve the original rollback failure. The connection will
            // remain unavailable until its owning scope is closed.
          }
        }
        resetMaterializationCommitState();
        if (db.isTransaction) {
          materializationBoundaryValid = false;
        }
      }
    };
    const makeExecutionError = (cause: unknown) =>
      new SqlError({
        reason: classifySqliteError(cause, {
          message: "Failed to execute statement",
          operation: "execute",
        }),
      });
    const validateSqlBeforePrepare = (sql: string) =>
      Effect.try({
        try: () => {
          const statement = parseMaterializationStatement(sql);
          if (statement._tag === "potentialMarkerDml") {
            handleMaterializationStatementFailure(statement);
            throw new Error("potential materialization marker DML could not be classified safely");
          }
          requireMaterializationMarkerTransaction(statement, snapshotMaterializationStatement());
          assertPersistentMarkerTarget(statement);
        },
        catch: makeExecutionError,
      });
    const hasRows = (statement: NodeSqlite.StatementSync): boolean => {
      const cached = statementReaderCache.get(statement);
      if (cached !== undefined) {
        return cached;
      }
      const value = statement.columns().length > 0;
      statementReaderCache.set(statement, value);
      return value;
    };
    const markerStatementChangedRows = (): boolean => {
      options._testHooks?.beforeMarkerChanges?.();
      const row = db.prepare("SELECT changes() AS changes").get() as
        | { readonly changes?: number | bigint }
        | undefined;
      const changes = row?.changes;
      if (typeof changes !== "number" && typeof changes !== "bigint") {
        throw new Error("SQLite did not return a valid marker statement change count");
      }
      return changes !== 0 && changes !== 0n;
    };
    const assertPersistentMarkerTarget = (statement: MaterializationStatement): void => {
      if (
        statement._tag !== "orchestrationMarker" &&
        statement._tag !== "coordinatorMarker" &&
        statement._tag !== "prepareMarker" &&
        statement._tag !== "initialPlanningHandoff" &&
        statement._tag !== "initialPlanningFinalization" &&
        statement._tag !== "implementationAdmission" &&
        statement._tag !== "implementationMaterialization" &&
        statement._tag !== "implementationTurnAcceptance" &&
        statement._tag !== "implementationStageStart" &&
        statement._tag !== "implementationStageFinalization" &&
        statement._tag !== "verificationAdmission" &&
        statement._tag !== "verificationMaterialization" &&
        statement._tag !== "verificationTurnAcceptance" &&
        statement._tag !== "verificationStageStart" &&
        statement._tag !== "verificationEvaluation" &&
        statement._tag !== "verificationStageFinalization" &&
        statement._tag !== "taskVerificationFinalization" &&
        statement._tag !== "runOnceStepMarker" &&
        !(statement._tag === "markerMutation" && statement.table !== undefined)
      ) {
        return;
      }
      const table =
        statement._tag === "orchestrationMarker"
          ? ORCHESTRATION_MARKER_TABLE
          : statement._tag === "coordinatorMarker"
            ? COORDINATOR_MARKER_TABLE
            : statement._tag === "prepareMarker"
              ? PREPARE_MARKER_TABLE
              : statement._tag === "runOnceStepMarker"
                ? RUN_ONCE_STEP_MARKER_TABLE
                : statement.table!;
      // Keep authority on the native connection and in the same synchronous
      // call stack as marker execution. These reads do not change changes().
      const mainEntry = db
        .prepare("SELECT type FROM main.sqlite_schema WHERE name = ? COLLATE NOCASE LIMIT 1")
        .get(table) as { readonly type?: string } | undefined;
      if (mainEntry?.type !== "table") {
        throw new Error(`persistent materialization marker table is missing or invalid: ${table}`);
      }
      if (statement.target === "main") {
        return;
      }
      const tempEntry = db
        .prepare("SELECT type FROM sqlite_temp_schema WHERE name = ? COLLATE NOCASE LIMIT 1")
        .get(table) as { readonly type?: string } | undefined;
      if (tempEntry !== undefined) {
        throw new Error(`temporary schema shadows materialization marker target: ${table}`);
      }
    };

    const prepareCache = yield* Cache.make({
      capacity: options.prepareCacheSize ?? 200,
      timeToLive: options.prepareCacheTTL ?? Duration.minutes(10),
      lookup: (sql: string) =>
        Effect.try({
          try: () => db.prepare(sql),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(cause, {
                message: "Failed to prepare statement",
                operation: "prepare",
              }),
            }),
        }),
    });

    const runStatement = <A>(
      statement: NodeSqlite.StatementSync,
      params: ReadonlyArray<unknown>,
      execute: (statement: NodeSqlite.StatementSync, params: ReadonlyArray<unknown>) => A,
    ) =>
      Effect.withFiber<A, SqlError>((fiber) => {
        const materializationStatement = parseMaterializationStatement(statement.sourceSQL);
        const snapshot = snapshotMaterializationStatement();
        try {
          requireMaterializationMarkerTransaction(materializationStatement, snapshot);
          ensureMaterializationCommitBoundary(materializationStatement, snapshot);
          assertPersistentMarkerTarget(materializationStatement);
          statement.setReadBigInts(Boolean(Context.get(fiber.context, Client.SafeIntegers)));
          const result = execute(statement, params);
          const markerWriteChangedRows =
            materializationStatement._tag === "orchestrationMarker" ||
            materializationStatement._tag === "coordinatorMarker" ||
            materializationStatement._tag === "prepareMarker" ||
            (materializationStatement._tag === "initialPlanningFinalization" &&
              materializationStatement.final) ||
            materializationStatement._tag === "implementationAdmission" ||
            materializationStatement._tag === "implementationMaterialization" ||
            materializationStatement._tag === "implementationTurnAcceptance" ||
            materializationStatement._tag === "implementationStageStart" ||
            materializationStatement._tag === "implementationStageFinalization" ||
            materializationStatement._tag === "verificationAdmission" ||
            materializationStatement._tag === "verificationMaterialization" ||
            materializationStatement._tag === "verificationTurnAcceptance" ||
            materializationStatement._tag === "verificationStageStart" ||
            materializationStatement._tag === "verificationEvaluation" ||
            materializationStatement._tag === "verificationStageFinalization" ||
            materializationStatement._tag === "taskVerificationFinalization" ||
            materializationStatement._tag === "runOnceStepMarker"
              ? markerStatementChangedRows()
              : false;
          const runPostCommitHook = updateMaterializationCommitBoundary(
            materializationStatement,
            snapshot,
            markerWriteChangedRows,
          );
          const transactionHooks = Context.get(fiber.context, NodeSqliteTransactionHooks);
          const afterAnyCommit =
            materializationStatement._tag === "commit"
              ? (transactionHooks.afterAnyCommitBeforeReturn?.() ?? Effect.void)
              : Effect.void;
          const afterMarkerCommit =
            runPostCommitHook && snapshot.boundary !== "verificationStageFinalization"
              ? transactionHooks.afterCommitBeforeReturn({
                  boundary: (snapshot.boundary === "prepare"
                    ? "agent-control-controlled-thread-prepare-finalization"
                    : snapshot.boundary === "initialPlanningFinalization"
                      ? "agent-control-initial-planning-stage-finalization"
                      : snapshot.boundary === "implementationAdmission"
                        ? "agent-control-implementation-admission"
                        : snapshot.boundary === "implementationMaterialization"
                          ? "agent-control-implementation-materialization"
                          : snapshot.boundary === "implementationTurnAcceptance"
                            ? "agent-control-implementation-turn-acceptance"
                            : snapshot.boundary === "implementationStageStart"
                              ? "agent-control-implementation-stage-start"
                              : snapshot.boundary === "implementationStageFinalization"
                                ? "agent-control-implementation-stage-finalization"
                                : snapshot.boundary === "verificationAdmission"
                                  ? "agent-control-verification-admission"
                                  : snapshot.boundary === "verificationMaterialization"
                                    ? "agent-control-verification-materialization"
                                    : snapshot.boundary === "verificationTurnAcceptance"
                                      ? "agent-control-verification-turn-acceptance"
                                      : snapshot.boundary === "verificationStageStart"
                                        ? "agent-control-verification-stage-start"
                                        : snapshot.boundary === "verificationEvaluation"
                                          ? "agent-control-verification-evaluation"
                                          : snapshot.boundary === "taskVerificationFinalization"
                                            ? "agent-control-task-verification-finalization"
                                            : "agent-control-controlled-thread-materialization-coordinator") as never,
                })
              : Effect.void;
          return afterAnyCommit.pipe(Effect.andThen(afterMarkerCommit), Effect.as(result));
        } catch (cause) {
          handleMaterializationStatementFailure(materializationStatement);
          return Effect.fail(makeExecutionError(cause));
        }
      });

    const prepareCached = (sql: string) =>
      Effect.andThen(validateSqlBeforePrepare(sql), Cache.get(prepareCache, sql)).pipe(
        Effect.tapError(() =>
          Effect.sync(() => {
            if (db.isTransaction && materializationCommitBoundary !== "open") {
              materializationBoundaryValid = false;
            }
            handleMaterializationStatementFailure(parseMaterializationStatement(sql));
          }),
        ),
      );
    const prepareUncached = (sql: string) =>
      Effect.andThen(
        validateSqlBeforePrepare(sql),
        Effect.try({
          try: () => db.prepare(sql),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(cause, {
                message: "Failed to prepare statement",
                operation: "prepare",
              }),
            }),
        }),
      ).pipe(
        Effect.tapError(() =>
          Effect.sync(() => {
            if (db.isTransaction && materializationCommitBoundary !== "open") {
              materializationBoundaryValid = false;
            }
            handleMaterializationStatementFailure(parseMaterializationStatement(sql));
          }),
        ),
      );

    const run = (sql: string, params: ReadonlyArray<unknown>, raw = false) =>
      Effect.flatMap(prepareCached(sql), (statement) =>
        runStatement(statement, params, (statement, params) => {
          if (hasRows(statement)) {
            return statement.all(...(params as any));
          }
          const result = statement.run(...(params as any));
          return raw ? (result as unknown as ReadonlyArray<any>) : [];
        }),
      );

    const runValues = (sql: string, params: ReadonlyArray<unknown>, unprepared = false) =>
      Effect.acquireUseRelease(
        unprepared ? prepareUncached(sql) : prepareCached(sql),
        (statement) =>
          runStatement(statement, params, (statement, params) => {
            if (hasRows(statement)) {
              statement.setReturnArrays(true);
              return statement.all(...(params as any)) as unknown as ReadonlyArray<
                ReadonlyArray<unknown>
              >;
            }
            statement.run(...(params as any));
            return [];
          }),
        (statement) =>
          Effect.try({
            try: () => {
              if (hasRows(statement)) {
                statement.setReturnArrays(false);
              }
            },
            catch: (cause) =>
              new SqlError({
                reason: classifySqliteError(cause, {
                  message: "Failed to reset statement result mode",
                  operation: "resetResultMode",
                }),
              }),
          }).pipe(Effect.orDie),
      );

    return identity<Connection>({
      execute(sql, params, rowTransform) {
        return rowTransform ? Effect.map(run(sql, params), rowTransform) : run(sql, params);
      },
      executeRaw(sql, params) {
        return run(sql, params, true);
      },
      executeValuesUnprepared(sql, params) {
        return runValues(sql, params, true);
      },
      executeValues(sql, params) {
        return runValues(sql, params);
      },
      executeUnprepared(sql, params, rowTransform) {
        const effect = prepareUncached(sql).pipe(
          Effect.flatMap((statement) =>
            runStatement(statement, params ?? [], (statement, params) => {
              if (hasRows(statement)) {
                return statement.all(...(params as any));
              }
              statement.run(...(params as any));
              return [];
            }),
          ),
        );
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeStream(_sql, _params) {
        return Stream.die(new UnsupportedNodeSqliteOperationError());
      },
    });
  });

  const semaphore = yield* Semaphore.make(1);
  const connection = yield* makeConnection;

  const acquirer = semaphore.withPermits(1)(Effect.succeed(connection));
  const transactionAcquirer = Effect.uninterruptibleMask((restore) => {
    const fiber = Fiber.getCurrent()!;
    const scope = Context.getUnsafe(fiber.context, Scope.Scope);
    return Effect.as(
      Effect.tap(restore(semaphore.take(1)), () => Scope.addFinalizer(scope, semaphore.release(1))),
      connection,
    );
  });

  return yield* Client.make({
    acquirer,
    compiler,
    transactionAcquirer,
    spanAttributes: [
      ...(options.spanAttributes ? Object.entries(options.spanAttributes) : []),
      [ATTR_DB_SYSTEM_NAME, "sqlite"],
    ],
    transformRows,
  });
});

const make = (
  options: SqliteClientInternalConfig,
): Effect.Effect<Client.SqlClient, SqlError, Scope.Scope | Reactivity.Reactivity> =>
  makeWithDatabase(
    options,
    () =>
      new NodeSqlite.DatabaseSync(options.filename, {
        readOnly: options.readonly ?? false,
        allowExtension: options.allowExtension ?? false,
      }),
  );

const makeMemory = (
  config: SqliteMemoryClientConfig = {},
): Effect.Effect<Client.SqlClient, SqlError, Scope.Scope | Reactivity.Reactivity> =>
  makeWithDatabase(
    {
      ...config,
      filename: ":memory:",
      readonly: false,
    },
    () => {
      const database = new NodeSqlite.DatabaseSync(":memory:", {
        allowExtension: config.allowExtension ?? false,
      });
      return database;
    },
  );

export const layerConfig = (
  config: Config.Wrap<SqliteClientConfig>,
): Layer.Layer<Client.SqlClient, Config.ConfigError | SqlError> =>
  Layer.effect(Client.SqlClient, Config.unwrap(config).pipe(Effect.flatMap(make))).pipe(
    Layer.provide(Reactivity.layer),
  );

export const layer = (config: SqliteClientConfig): Layer.Layer<Client.SqlClient, SqlError> =>
  Layer.effect(Client.SqlClient, make(config)).pipe(Layer.provide(Reactivity.layer));

/** @internal Test-only client constructor for connection-registration failure coverage. */
export const layerTest = (
  config: SqliteClientInternalConfig,
): Layer.Layer<Client.SqlClient, SqlError> =>
  Layer.effect(Client.SqlClient, make(config)).pipe(Layer.provide(Reactivity.layer));

export const layerMemory = (
  config: SqliteMemoryClientConfig = {},
): Layer.Layer<Client.SqlClient, SqlError> =>
  Layer.effect(Client.SqlClient, makeMemory(config)).pipe(Layer.provide(Reactivity.layer));
