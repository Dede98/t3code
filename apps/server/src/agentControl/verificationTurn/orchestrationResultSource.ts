import * as NodeCrypto from "node:crypto";

import {
  OrchestrationActorKind,
  OrchestrationEvent,
  type MessageId,
  type ProviderInstanceId,
  type ThreadId,
  type TurnId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  type JsonValue,
} from "../initialPlanning/eventEvidence.ts";
import type { AgentControlVerificationClaim } from "./model.ts";
import { AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION } from "./prompt.ts";
import { loadVerificationTerminalFromOrchestrationHistory } from "./orchestrationTerminalHistory.ts";
import type { AgentControlVerificationTurnAcceptance } from "./Services/AgentControlVerificationHandoffStore.ts";
import { AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION } from "./verificationResult.ts";

export class VerificationResultHistoryError extends Schema.TaggedErrorClass<VerificationResultHistoryError>()(
  "VerificationResultHistoryError",
  {
    operation: Schema.String,
    reason: Schema.Literals(["persistence", "corrupt-history", "authority-conflict"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface SealableVerificationResultSource {
  readonly sourceDisposition: "captured" | "missing" | "oversize";
  readonly finalMessageId: MessageId | null;
  readonly bytes: Uint8Array;
  readonly outputDigest: string | null;
  readonly outputByteLength: number;
  readonly sourceEventId: string | null;
  readonly sourceEventSequence: number | null;
  readonly sourceEventStreamVersion: number | null;
}

export interface VerificationResultSource extends SealableVerificationResultSource {
  readonly terminalEventId: string;
  readonly terminalEventSequence: number;
  readonly terminalEventStreamVersion: number;
}

interface StoredResultFragmentEvent {
  readonly event: Extract<
    OrchestrationEvent,
    {
      readonly type: "thread.message-sent" | "thread.verification-result-fragment-captured";
    }
  >;
  readonly actorKind: typeof OrchestrationActorKind.Type;
  readonly streamVersion: number;
}

interface VerificationResultCaptureIdentity {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerTurnId: TurnId;
  readonly afterStreamVersion: number;
  readonly sealedAtStreamVersion?: number;
  readonly handoffId?: string;
  readonly providerDeliveryId?: string;
  readonly resultSchemaFingerprint?: string;
}

const historyError = (
  operation: string,
  reason: VerificationResultHistoryError["reason"],
  cause?: unknown,
) =>
  new VerificationResultHistoryError({
    operation,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const decodeText = (value: unknown, operation: string) =>
  Effect.try({
    try: () => decodeCanonicalUtf8Bytes(value),
    catch: (cause) => historyError(operation, "corrupt-history", cause),
  });

const decodeNullableText = (value: unknown, operation: string) =>
  value === null ? Effect.succeed(null) : decodeText(value, operation);

const decodeUnknownJson = Schema.decodeUnknownEffect(Schema.UnknownFromJsonString);
const decodeJson = (value: unknown, operation: string) =>
  decodeText(value, `${operation}-bytes`).pipe(
    Effect.flatMap((source) =>
      decodeUnknownJson(source).pipe(
        Effect.map((decoded) => decoded as JsonValue),
        Effect.mapError((cause) => historyError(`${operation}-json`, "corrupt-history", cause)),
      ),
    ),
  );

const routingBytes = (value: string): Uint8Array => new TextEncoder().encode(value);
const sha256Bytes = (bytes: Uint8Array): string =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");

const loadVerificationResultCaptureSnapshot = Effect.fn("loadVerificationResultCaptureSnapshot")(
  function* (sql: SqlClient.SqlClient, identity: VerificationResultCaptureIdentity) {
    const threadBytes = routingBytes(identity.threadId);
    const aggregateKindBytes = routingBytes("thread");
    const rawRows = yield* sql<Record<string, unknown>>`
    SELECT sequence, stream_version AS "streamVersion",
      typeof(event_id) AS "eventIdStorageClass", CAST(event_id AS BLOB) AS "eventIdBytes",
      typeof(aggregate_kind) AS "aggregateKindStorageClass",
      CAST(aggregate_kind AS BLOB) AS "aggregateKindBytes",
      typeof(stream_id) AS "aggregateIdStorageClass",
      CAST(stream_id AS BLOB) AS "aggregateIdBytes",
      typeof(event_type) AS "eventTypeStorageClass",
      CAST(event_type AS BLOB) AS "eventTypeBytes",
      typeof(occurred_at) AS "occurredAtStorageClass",
      CAST(occurred_at AS BLOB) AS "occurredAtBytes",
      typeof(command_id) AS "commandIdStorageClass",
      CASE WHEN command_id IS NULL THEN NULL ELSE CAST(command_id AS BLOB) END AS "commandIdBytes",
      typeof(causation_event_id) AS "causationEventIdStorageClass",
      CASE WHEN causation_event_id IS NULL THEN NULL ELSE CAST(causation_event_id AS BLOB) END
        AS "causationEventIdBytes",
      typeof(correlation_id) AS "correlationIdStorageClass",
      CASE WHEN correlation_id IS NULL THEN NULL ELSE CAST(correlation_id AS BLOB) END
        AS "correlationIdBytes",
      typeof(actor_kind) AS "actorKindStorageClass",
      CAST(actor_kind AS BLOB) AS "actorKindBytes",
      typeof(payload_json) AS "payloadStorageClass",
      CAST(payload_json AS BLOB) AS "payloadBytes",
      typeof(metadata_json) AS "metadataStorageClass",
      CAST(metadata_json AS BLOB) AS "metadataBytes"
    FROM orchestration_events
    WHERE CAST(aggregate_kind AS BLOB) = ${aggregateKindBytes}
      AND CAST(stream_id AS BLOB) = ${threadBytes}
    ORDER BY stream_version, sequence
  `.pipe(
      Effect.mapError((cause) => historyError("read-result-source-history", "persistence", cause)),
    );
    if (rawRows.length === 0) {
      return yield* historyError("result-source-history-missing", "corrupt-history");
    }

    const decodeEvent = Schema.decodeUnknownEffect(OrchestrationEvent);
    const decodeActor = Schema.decodeUnknownEffect(OrchestrationActorKind);
    const fragments: Array<StoredResultFragmentEvent> = [];
    let previousSequence = 0;
    for (const [index, row] of rawRows.entries()) {
      if (
        typeof row.sequence !== "number" ||
        !Number.isInteger(row.sequence) ||
        row.sequence <= previousSequence ||
        typeof row.streamVersion !== "number" ||
        !Number.isInteger(row.streamVersion) ||
        row.streamVersion !== index + 1
      ) {
        return yield* historyError("result-source-history-order", "corrupt-history");
      }
      previousSequence = row.sequence;
      for (const [storageClass, operation] of [
        [row.eventIdStorageClass, "result-source-event-id-storage"],
        [row.aggregateKindStorageClass, "result-source-aggregate-kind-storage"],
        [row.aggregateIdStorageClass, "result-source-aggregate-id-storage"],
        [row.eventTypeStorageClass, "result-source-event-type-storage"],
        [row.occurredAtStorageClass, "result-source-occurred-at-storage"],
        [row.actorKindStorageClass, "result-source-actor-kind-storage"],
        [row.payloadStorageClass, "result-source-payload-storage"],
        [row.metadataStorageClass, "result-source-metadata-storage"],
      ] as const) {
        if (storageClass !== "text") {
          return yield* historyError(operation, "corrupt-history");
        }
      }
      for (const [storageClass, operation] of [
        [row.commandIdStorageClass, "result-source-command-id-storage"],
        [row.causationEventIdStorageClass, "result-source-causation-storage"],
        [row.correlationIdStorageClass, "result-source-correlation-storage"],
      ] as const) {
        if (storageClass !== "text" && storageClass !== "null") {
          return yield* historyError(operation, "corrupt-history");
        }
      }
      const [
        eventId,
        aggregateKind,
        aggregateId,
        eventType,
        occurredAt,
        commandId,
        causationEventId,
        correlationId,
        actorKindText,
        payload,
        metadata,
      ] = yield* Effect.all([
        decodeText(row.eventIdBytes, "result-source-event-id"),
        decodeText(row.aggregateKindBytes, "result-source-aggregate-kind"),
        decodeText(row.aggregateIdBytes, "result-source-aggregate-id"),
        decodeText(row.eventTypeBytes, "result-source-event-type"),
        decodeText(row.occurredAtBytes, "result-source-occurred-at"),
        decodeNullableText(row.commandIdBytes, "result-source-command-id"),
        decodeNullableText(row.causationEventIdBytes, "result-source-causation-id"),
        decodeNullableText(row.correlationIdBytes, "result-source-correlation-id"),
        decodeText(row.actorKindBytes, "result-source-actor-kind"),
        decodeJson(row.payloadBytes, "result-source-payload"),
        decodeJson(row.metadataBytes, "result-source-metadata"),
      ]);
      if (aggregateKind !== "thread" || aggregateId !== identity.threadId) {
        return yield* historyError("result-source-routing", "corrupt-history");
      }
      const actorKind = yield* decodeActor(actorKindText).pipe(
        Effect.mapError((cause) =>
          historyError("result-source-decode-actor", "corrupt-history", cause),
        ),
      );
      const event = yield* decodeEvent({
        sequence: row.sequence,
        eventId,
        aggregateKind,
        aggregateId,
        type: eventType,
        occurredAt,
        commandId,
        causationEventId,
        correlationId,
        payload,
        metadata,
      }).pipe(
        Effect.mapError((cause) =>
          historyError("result-source-decode-event", "corrupt-history", cause),
        ),
      );
      if (
        canonicalJson(event.payload as JsonValue) !== canonicalJson(payload) ||
        canonicalJson(event.metadata as JsonValue) !== canonicalJson(metadata)
      ) {
        return yield* historyError("result-source-event-fields-stripped", "corrupt-history");
      }
      if (
        event.type === "thread.message-sent" ||
        event.type === "thread.verification-result-fragment-captured"
      ) {
        fragments.push({ event, actorKind, streamVersion: row.streamVersion });
      }
    }

    const byMessage = new Map<
      MessageId,
      { text: string; complete: StoredResultFragmentEvent | null; latestStreamVersion: number }
    >();
    for (const entry of fragments) {
      const { event } = entry;
      const correlation = event.metadata.providerRuntimeMessage;
      const capture = event.metadata.verificationResultCapture;
      const mentionsTurn =
        event.type === "thread.message-sent"
          ? event.payload.role === "assistant" && event.payload.turnId === identity.providerTurnId
          : event.payload.turnId === identity.providerTurnId;
      const correlatedTurn = correlation?.providerTurnId === identity.providerTurnId;
      if (!mentionsTurn && !correlatedTurn) continue;
      if (
        entry.streamVersion <= identity.afterStreamVersion ||
        event.payload.threadId !== identity.threadId ||
        event.payload.turnId !== identity.providerTurnId ||
        entry.actorKind !== "provider" ||
        correlation === undefined ||
        correlation.providerInstanceId !== identity.providerInstanceId ||
        correlation.providerTurnId !== identity.providerTurnId ||
        event.commandId === null ||
        !event.commandId.startsWith(`provider:${correlation.runtimeEventId}:`) ||
        event.causationEventId !== null ||
        event.correlationId !== event.commandId
      ) {
        return yield* historyError("result-source-message-identity", "authority-conflict");
      }
      if (event.type === "thread.message-sent" && event.payload.role !== "assistant") {
        return yield* historyError("result-source-message-role", "authority-conflict");
      }
      if (
        identity.sealedAtStreamVersion !== undefined &&
        entry.streamVersion > identity.sealedAtStreamVersion
      ) {
        return yield* historyError("result-source-message-after-seal", "authority-conflict");
      }
      if (capture !== undefined) {
        if (
          capture.schemaVersion !== 1 ||
          capture.providerInstanceId !== identity.providerInstanceId ||
          capture.providerTurnId !== identity.providerTurnId ||
          (identity.handoffId !== undefined && capture.handoffId !== identity.handoffId) ||
          (identity.providerDeliveryId !== undefined &&
            capture.providerDeliveryId !== identity.providerDeliveryId) ||
          (identity.resultSchemaFingerprint !== undefined &&
            capture.resultSchemaFingerprint !== identity.resultSchemaFingerprint)
        ) {
          return yield* historyError("result-source-capture-authority", "authority-conflict");
        }
        if (event.type === "thread.message-sent") {
          if (capture.disposition !== "presentation") {
            return yield* historyError(
              "result-source-presentation-authority",
              "authority-conflict",
            );
          }
          continue;
        }
        if (capture.disposition !== "authority") {
          return yield* historyError("result-source-fragment-authority", "authority-conflict");
        }
      } else if (event.type === "thread.verification-result-fragment-captured") {
        return yield* historyError("result-source-fragment-authority", "authority-conflict");
      }
      const state = byMessage.get(event.payload.messageId) ?? {
        text: "",
        complete: null,
        latestStreamVersion: entry.streamVersion,
      };
      if (state.complete !== null) {
        return yield* historyError("result-source-message-after-completion", "authority-conflict");
      }
      const isDelta =
        event.type === "thread.message-sent"
          ? event.payload.streaming
          : event.payload.fragment.kind === "delta";
      if (isDelta) {
        const text =
          event.type === "thread.message-sent"
            ? event.payload.text
            : event.payload.fragment.kind === "delta"
              ? event.payload.fragment.text
              : "";
        if (
          event.type === "thread.verification-result-fragment-captured" &&
          correlation.runtimeEventType !== "content.delta"
        ) {
          return yield* historyError("result-source-delta-runtime-event", "authority-conflict");
        }
        state.text += text;
      } else {
        if (
          (event.type === "thread.message-sent" && event.payload.text !== "") ||
          (event.type === "thread.verification-result-fragment-captured" &&
            correlation.runtimeEventType !== "item.completed" &&
            correlation.runtimeEventType !== "turn.completed" &&
            correlation.runtimeEventType !== "request.opened" &&
            correlation.runtimeEventType !== "user-input.requested")
        ) {
          return yield* historyError("result-source-completion-payload", "corrupt-history");
        }
        state.complete = entry;
      }
      state.latestStreamVersion = entry.streamVersion;
      byMessage.set(event.payload.messageId, state);
    }

    return byMessage;
  },
);

export const loadVerificationResultCapturedMessage = Effect.fn(
  "loadVerificationResultCapturedMessage",
)(function* (
  sql: SqlClient.SqlClient,
  identity: VerificationResultCaptureIdentity,
  messageId: MessageId,
) {
  const messages = yield* loadVerificationResultCaptureSnapshot(sql, identity);
  const message = messages.get(messageId);
  return message === undefined
    ? null
    : { text: message.text, completed: message.complete !== null };
});

export const loadOpenVerificationResultMessageIds = Effect.fn(
  "loadOpenVerificationResultMessageIds",
)(function* (sql: SqlClient.SqlClient, identity: VerificationResultCaptureIdentity) {
  const messages = yield* loadVerificationResultCaptureSnapshot(sql, identity);
  return [...messages.entries()]
    .filter(([, state]) => state.complete === null)
    .map(([messageId]) => messageId);
});

export const loadSealableVerificationResultSource = Effect.fn(
  "loadSealableVerificationResultSource",
)(function* (sql: SqlClient.SqlClient, identity: VerificationResultCaptureIdentity) {
  const byMessage = yield* loadVerificationResultCaptureSnapshot(sql, identity);

  const completed = [...byMessage.entries()]
    .filter(
      (entry): entry is [MessageId, (typeof entry)[1] & { complete: StoredResultFragmentEvent }] =>
        entry[1].complete !== null,
    )
    .sort((left, right) => left[1].complete.streamVersion - right[1].complete.streamVersion);
  const selected = completed.at(-1);
  if (selected === undefined) {
    if (byMessage.size !== 0) {
      return yield* historyError("result-source-open-message", "authority-conflict");
    }
    return {
      sourceDisposition: "missing",
      finalMessageId: null,
      bytes: new Uint8Array(),
      outputDigest: null,
      outputByteLength: 0,
      sourceEventId: null,
      sourceEventSequence: null,
      sourceEventStreamVersion: null,
    } satisfies SealableVerificationResultSource;
  }
  for (const [, state] of byMessage) {
    if (state.complete === null || state.latestStreamVersion > selected[1].complete.streamVersion) {
      return yield* historyError("result-source-later-open-message", "authority-conflict");
    }
  }
  const bytes = new TextEncoder().encode(selected[1].text);
  const complete = selected[1].complete;
  return {
    sourceDisposition: bytes.byteLength > 64 * 1024 ? "oversize" : "captured",
    finalMessageId: selected[0],
    bytes,
    outputDigest: sha256Bytes(bytes),
    outputByteLength: bytes.byteLength,
    sourceEventId: complete.event.eventId,
    sourceEventSequence: complete.event.sequence,
    sourceEventStreamVersion: complete.streamVersion,
  } satisfies SealableVerificationResultSource;
});

export const loadVerificationResultSource = Effect.fn("loadVerificationResultSource")(function* (
  sql: SqlClient.SqlClient,
  claim: AgentControlVerificationClaim,
) {
  if (
    claim.evidence.templateVersion !== AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION ||
    claim.evidence.resultSchemaVersion !== AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION ||
    claim.evidence.promptContractFingerprint === null ||
    claim.evidence.resultSchemaFingerprint === null ||
    claim.delivery.state !== "completed" ||
    claim.delivery.providerTurnId === null ||
    claim.delivery.terminalEventId === null ||
    claim.delivery.terminalProviderState !== "completed"
  ) {
    return { _tag: "Waiting" } as const;
  }
  const authorityRows = yield* sql<Record<string, unknown>>`
      SELECT accepted.*,
        typeof(accepted.handoff_id) AS "handoffIdStorage",
        typeof(accepted.message_event_sequence) AS "messageSequenceStorage",
        typeof(accepted.turn_request_event_sequence) AS "turnSequenceStorage",
        typeof(stage.status) AS "stageStatusStorage",
        typeof(stage.revision) AS "stageRevisionStorage",
        typeof(lease.status) AS "leaseStatusStorage",
        typeof(lease.fence_token) AS "fenceTokenStorage",
        typeof(evidence.handoff_id) AS "startHandoffStorage",
        typeof(evidence.provider_delivery_id) AS "startDeliveryStorage",
        typeof(evidence.provider_instance_id) AS "startProviderStorage",
        typeof(evidence.provider_turn_id) AS "startTurnStorage",
        typeof(evidence.thread_id) AS "startThreadStorage",
        typeof(evidence.stage_run_id) AS "startStageRunStorage",
        typeof(evidence.attempt_id) AS "startAttemptStorage",
        typeof(evidence.lease_id) AS "startLeaseStorage",
        typeof(started.start_evidence_id) AS "startMarkerEvidenceStorage",
        typeof(started.provider_delivery_id) AS "startMarkerDeliveryStorage",
        typeof(stage.stage_run_id) AS "stageRunStorage",
        typeof(lease.lease_id) AS "leaseIdStorage",
        evidence.handoff_id AS "startHandoffId",
        evidence.provider_delivery_id AS "startProviderDeliveryId",
        evidence.provider_instance_id AS "startProviderInstanceId",
        evidence.provider_turn_id AS "startProviderTurnId",
        evidence.thread_id AS "startThreadId",
        evidence.stage_run_id AS "startStageRunId",
        evidence.attempt_id AS "startAttemptId",
        evidence.lease_id AS "startLeaseId",
        stage.status AS "stageStatus", stage.revision AS "stageRevision",
        lease.status AS "leaseStatus", lease.fence_token AS "leaseFenceToken",
        lease.holder_id AS "leaseHolderId",
        started.start_marker_id AS "startMarkerId",
        started.provider_delivery_id AS "startedProviderDeliveryId"
      FROM agent_control_verification_turn_accepted accepted
      JOIN agent_control_verification_stage_started_evidence evidence
        ON CAST(evidence.handoff_id AS BLOB) = CAST(accepted.handoff_id AS BLOB)
      JOIN agent_control_verification_stage_started_markers started
        ON CAST(started.start_evidence_id AS BLOB) = CAST(evidence.start_evidence_id AS BLOB)
      JOIN agent_control_stage_run_states stage
        ON CAST(stage.stage_run_id AS BLOB) = CAST(evidence.stage_run_id AS BLOB)
      JOIN agent_control_stage_run_lease_states lease
        ON CAST(lease.lease_id AS BLOB) = CAST(evidence.lease_id AS BLOB)
      WHERE CAST(accepted.handoff_id AS BLOB) = ${routingBytes(claim.evidence.handoffId)}
    `.pipe(Effect.mapError((cause) => historyError("load-result-authority", "persistence", cause)));
  if (authorityRows.length === 0) return { _tag: "Waiting" } as const;
  if (authorityRows.length !== 1) {
    return yield* historyError("load-result-authority-count", "corrupt-history");
  }
  const authority = authorityRows[0]!;
  if (
    authority.handoffIdStorage !== "text" ||
    authority.messageSequenceStorage !== "integer" ||
    authority.turnSequenceStorage !== "integer" ||
    authority.stageStatusStorage !== "text" ||
    authority.stageRevisionStorage !== "integer" ||
    authority.leaseStatusStorage !== "text" ||
    authority.fenceTokenStorage !== "integer" ||
    authority.startHandoffStorage !== "text" ||
    authority.startDeliveryStorage !== "text" ||
    authority.startProviderStorage !== "text" ||
    authority.startTurnStorage !== "text" ||
    authority.startThreadStorage !== "text" ||
    authority.startStageRunStorage !== "text" ||
    authority.startAttemptStorage !== "text" ||
    authority.startLeaseStorage !== "text" ||
    authority.startMarkerEvidenceStorage !== "text" ||
    authority.startMarkerDeliveryStorage !== "text" ||
    authority.stageRunStorage !== "text" ||
    authority.leaseIdStorage !== "text" ||
    authority.stageStatus !== "running" ||
    authority.stageRevision !== 2 ||
    authority.leaseStatus !== "reserved" ||
    authority.leaseFenceToken !== claim.evidence.fenceToken ||
    authority.leaseHolderId !== claim.evidence.leaseHolderId ||
    authority.startedProviderDeliveryId !== claim.evidence.providerDeliveryId ||
    authority.handoff_id !== claim.evidence.handoffId ||
    authority.thread_id !== claim.evidence.threadId ||
    authority.startHandoffId !== claim.evidence.handoffId ||
    authority.startProviderDeliveryId !== claim.evidence.providerDeliveryId ||
    authority.startProviderInstanceId !== claim.evidence.providerInstanceId ||
    authority.startProviderTurnId !== claim.delivery.providerTurnId ||
    authority.startThreadId !== claim.evidence.threadId ||
    authority.startStageRunId !== claim.evidence.stageRunId ||
    authority.startAttemptId !== claim.evidence.attemptId ||
    authority.startLeaseId !== claim.evidence.leaseId
  ) {
    return yield* historyError("load-result-authority-divergent", "authority-conflict");
  }
  const acceptance = {
    handoffId: authority.handoff_id,
    handoffFingerprint: authority.handoff_fingerprint,
    controlledThreadReservationId: authority.controlled_thread_reservation_id,
    threadId: authority.thread_id,
    planningThreadId: authority.planning_thread_id,
    planId: authority.plan_id,
    turnRequestCommandId: authority.turn_request_command_id,
    messageId: authority.message_id,
    messageEventId: authority.message_event_id,
    messageEventSequence: authority.message_event_sequence,
    turnRequestEventId: authority.turn_request_event_id,
    turnRequestEventSequence: authority.turn_request_event_sequence,
    messageEventEnvelopeJson: authority.message_event_envelope_json,
    turnRequestEventEnvelopeJson: authority.turn_request_event_envelope_json,
    eventEvidenceDigest: authority.event_evidence_digest,
    acceptedAt: authority.accepted_at,
  } as AgentControlVerificationTurnAcceptance;
  const terminal = yield* loadVerificationTerminalFromOrchestrationHistory(
    sql,
    claim,
    acceptance,
  ).pipe(
    Effect.mapError((cause) =>
      historyError(
        cause.operation,
        cause.reason === "persistence" ? "persistence" : "authority-conflict",
        cause,
      ),
    ),
  );
  if (terminal._tag === "Waiting") return terminal;
  const seal = terminal.resultSourceSeal;
  if (
    seal === undefined ||
    terminal.observation.runtimeEventId !== claim.delivery.terminalEventId ||
    terminal.observation.deliveryState !== "completed"
  ) {
    return yield* historyError("load-result-terminal-authority", "authority-conflict");
  }
  const source = yield* loadSealableVerificationResultSource(sql, {
    threadId: claim.evidence.threadId,
    providerInstanceId: claim.evidence.providerInstanceId,
    providerTurnId: seal.providerTurnId,
    afterStreamVersion: 4,
    sealedAtStreamVersion: terminal.terminalStreamVersion,
    handoffId: claim.evidence.handoffId,
    providerDeliveryId: claim.evidence.providerDeliveryId,
    resultSchemaFingerprint: claim.evidence.resultSchemaFingerprint,
  });
  if (
    seal.sourceDisposition !== source.sourceDisposition ||
    seal.finalMessageId !== source.finalMessageId ||
    seal.outputDigest !== source.outputDigest ||
    seal.outputByteLength !== source.outputByteLength
  ) {
    return yield* historyError("load-result-terminal-seal-divergent", "authority-conflict");
  }
  return {
    _tag: "Ready",
    source: {
      ...source,
      terminalEventId: terminal.terminalEventId,
      terminalEventSequence: terminal.terminalEventSequence,
      terminalEventStreamVersion: terminal.terminalStreamVersion,
    } satisfies VerificationResultSource,
  } as const;
});
