import * as NodeCrypto from "node:crypto";

import {
  OrchestrationActorKind,
  OrchestrationEvent,
  type EventId,
  type MessageId,
  type ProviderInstanceId,
  type ThreadId,
  type TurnId,
  type VerificationResultFragment,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  type JsonValue,
} from "../initialPlanning/eventEvidence.ts";
import { normalizeLegacyProviderRuntimeMessageCorrelationMetadata } from "../../orchestration/providerRuntimeMessageCorrelation.ts";
import type { AgentControlVerificationClaim } from "./model.ts";
import { AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION } from "./prompt.ts";
import { loadVerificationTerminalFromOrchestrationHistory } from "./orchestrationTerminalHistory.ts";
import type { AgentControlVerificationTurnAcceptance } from "./Services/AgentControlVerificationHandoffStore.ts";
import {
  AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES,
  AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION,
} from "./verificationResult.ts";
import {
  VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS,
  verificationResultCompletionDetailDigest,
  verificationResultDeltaTextDigest,
  verificationResultOutputEvidenceDigest,
} from "./runtimeEvidence.ts";

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
  readonly sourceEventId: EventId | null;
  readonly sourceEventSequence: number | null;
  readonly sourceEventStreamVersion: number | null;
}

/**
 * `outputDigest` above is the SHA-256 of the reconstructable raw output bytes.
 * It remains null for oversize output. Capture fragments separately carry a
 * domain-separated evidence chain over every full fragment digest, length,
 * presence bit, kind, and ordinal. That chain is immutable replay evidence;
 * it is deliberately not described as a raw-output digest.
 */

export interface VerificationResultSource extends SealableVerificationResultSource {
  readonly terminalEventId: string;
  readonly terminalEventSequence: number;
  readonly terminalEventStreamVersion: number;
}

interface StoredResultCaptureEvent {
  readonly event: Extract<
    OrchestrationEvent,
    { readonly type: "thread.verification-result-fragment-captured" }
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

export const AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL =
  AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES + 1;
const RESULT_SOURCE_PAGE_SIZE = 32;

const saturatingResultByteLength = (previous: number, next: number): number =>
  previous >= AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL ||
  next > AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL - previous
    ? AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL
    : previous + next;

const utf8Prefix = (value: string, byteBudget: number): string => {
  if (byteBudget <= 0 || value.length === 0) return "";
  if (Buffer.byteLength(value, "utf8") <= byteBudget) return value;
  let bytes = 0;
  let offset = 0;
  while (offset < value.length) {
    const codePoint = value.codePointAt(offset)!;
    const width = codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
    if (bytes + width > byteBudget) break;
    bytes += width;
    offset += codePoint > 0xffff ? 2 : 1;
  }
  return value.slice(0, offset);
};

export const makeBoundedVerificationResultDelta = (
  text: string,
  previous: {
    readonly outputByteLength: number;
    readonly storedByteLength: number;
    readonly fragmentOrdinal: number;
    readonly cumulativeEvidenceDigest: string;
  } | null,
): Extract<VerificationResultFragment, { readonly kind: "delta" }> => {
  const fullTextByteLength = Buffer.byteLength(text, "utf8");
  const previousOutputByteLength = previous?.outputByteLength ?? 0;
  const previousStoredByteLength = previous?.storedByteLength ?? 0;
  const remainingBudget = Math.max(
    0,
    AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES - previousStoredByteLength,
  );
  const textPrefix = utf8Prefix(text, remainingBudget);
  const prefixByteLength = Buffer.byteLength(textPrefix, "utf8");
  const fullTextDigest = verificationResultDeltaTextDigest(text);
  const fragmentOrdinal = (previous?.fragmentOrdinal ?? 0) + 1;
  return {
    kind: "delta" as const,
    textPrefix,
    prefixByteLength,
    fullTextByteLength,
    fullTextDigest,
    cumulativeSourceByteLength: saturatingResultByteLength(
      previousOutputByteLength,
      fullTextByteLength,
    ),
    fragmentOrdinal,
    cumulativeEvidenceDigest: verificationResultOutputEvidenceDigest({
      previousDigest:
        previous?.cumulativeEvidenceDigest ?? VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS,
      fragmentKind: "delta",
      fragmentOrdinal,
      fullByteLength: fullTextByteLength,
      fullDigest: fullTextDigest,
      detailPresent: true,
    }),
  };
};

export const makeBoundedVerificationResultCompletion = (
  completionText: string | null,
  previous: {
    readonly outputByteLength: number;
    readonly storedByteLength: number;
    readonly fragmentOrdinal: number;
    readonly cumulativeEvidenceDigest: string;
  } | null,
): Extract<VerificationResultFragment, { readonly kind: "completion" }> => {
  const detail =
    completionText === null
      ? ({ present: false } as const)
      : ({
          present: true,
          fullByteLength: Buffer.byteLength(completionText, "utf8"),
          fullDigest: verificationResultCompletionDetailDigest(completionText),
        } as const);
  const fragmentOrdinal = (previous?.fragmentOrdinal ?? 0) + 1;
  const completionTextPrefix =
    completionText === null || previous !== null
      ? null
      : utf8Prefix(completionText, AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES);
  const outputByteLength =
    previous?.outputByteLength ??
    (detail.present ? saturatingResultByteLength(0, detail.fullByteLength) : 0);
  return {
    kind: "completion" as const,
    completionTextPrefix,
    outputByteLength,
    completionDetail: detail,
    fragmentOrdinal,
    cumulativeEvidenceDigest: verificationResultOutputEvidenceDigest({
      previousDigest:
        previous?.cumulativeEvidenceDigest ?? VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS,
      fragmentKind: "completion",
      fragmentOrdinal,
      fullByteLength: detail.present ? detail.fullByteLength : null,
      fullDigest: detail.present ? detail.fullDigest : null,
      detailPresent: detail.present,
    }),
  };
};

interface ScannedResultMessage {
  readonly messageId: MessageId;
  readonly bytes: Uint8Array;
  readonly outputByteLength: number;
  readonly fragmentOrdinal: number;
  readonly cumulativeEvidenceDigest: string;
  readonly complete: StoredResultCaptureEvent | null;
}

const loadVerificationResultCaptureSnapshot = Effect.fn("loadVerificationResultCaptureSnapshot")(
  function* (
    sql: SqlClient.SqlClient,
    identity: VerificationResultCaptureIdentity,
    options?: {
      readonly beforeRuntimeEventId?: EventId;
    },
  ) {
    const threadBytes = routingBytes(identity.threadId);
    const aggregateKindBytes = routingBytes("thread");
    const decodeEvent = Schema.decodeUnknownEffect(OrchestrationEvent);
    const decodeActor = Schema.decodeUnknownEffect(OrchestrationActorKind);
    const buffer = new Uint8Array(AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES);
    let cursor = 0;
    let previousSequence = 0;
    let previousStreamVersion = 0;
    let openMessageId: MessageId | null = null;
    let openStoredByteLength = 0;
    let openOutputByteLength = 0;
    let openFragmentOrdinal = 0;
    let openCumulativeEvidenceDigest = VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS;
    let selected: ScannedResultMessage | null = null;
    const currentSnapshot = () => ({
      selected,
      open:
        openMessageId === null
          ? null
          : {
              messageId: openMessageId,
              bytes: buffer.subarray(0, openStoredByteLength),
              outputByteLength: openOutputByteLength,
              fragmentOrdinal: openFragmentOrdinal,
              cumulativeEvidenceDigest: openCumulativeEvidenceDigest,
              complete: null,
            },
    });

    while (true) {
      const rawRows = yield* sql<Record<string, unknown>>`
        SELECT typeof(sequence) AS "sequenceStorageClass", sequence,
          typeof(stream_version) AS "streamVersionStorageClass",
          stream_version AS "streamVersion",
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
          CASE WHEN command_id IS NULL THEN NULL ELSE CAST(command_id AS BLOB) END
            AS "commandIdBytes",
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
        FROM main.orchestration_events
        WHERE sequence > ${cursor}
          AND CAST(aggregate_kind AS BLOB) = ${aggregateKindBytes}
          AND CAST(stream_id AS BLOB) = ${threadBytes}
        ORDER BY sequence
        LIMIT ${RESULT_SOURCE_PAGE_SIZE}
      `.pipe(
        Effect.mapError((cause) =>
          historyError("read-result-source-history", "persistence", cause),
        ),
      );
      if (rawRows.length === 0) break;

      for (const row of rawRows) {
        if (
          row.sequenceStorageClass !== "integer" ||
          typeof row.sequence !== "number" ||
          !Number.isInteger(row.sequence) ||
          row.sequence <= previousSequence ||
          row.streamVersionStorageClass !== "integer" ||
          typeof row.streamVersion !== "number" ||
          !Number.isInteger(row.streamVersion) ||
          row.streamVersion !== previousStreamVersion + 1
        ) {
          return yield* historyError("result-source-history-order", "corrupt-history");
        }
        previousSequence = row.sequence;
        previousStreamVersion = row.streamVersion;
        cursor = row.sequence;
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
          canonicalJson(event.metadata as JsonValue) !==
            canonicalJson(
              normalizeLegacyProviderRuntimeMessageCorrelationMetadata(metadata) as JsonValue,
            )
        ) {
          return yield* historyError("result-source-event-fields-stripped", "corrupt-history");
        }
        if (
          event.type !== "thread.message-sent" &&
          event.type !== "thread.verification-result-fragment-captured"
        ) {
          continue;
        }
        const correlation = event.metadata.providerRuntimeMessage;
        const capture = event.metadata.verificationResultCapture;
        const mentionsTurn =
          event.type === "thread.message-sent"
            ? event.payload.role === "assistant" && event.payload.turnId === identity.providerTurnId
            : event.payload.turnId === identity.providerTurnId;
        const correlatedTurn = correlation?.providerTurnId === identity.providerTurnId;
        if (!mentionsTurn && !correlatedTurn) continue;
        if (
          row.streamVersion <= identity.afterStreamVersion ||
          event.payload.threadId !== identity.threadId ||
          event.payload.turnId !== identity.providerTurnId ||
          actorKind !== "provider" ||
          correlation === undefined ||
          correlation.providerInstanceId !== identity.providerInstanceId ||
          correlation.providerTurnId !== identity.providerTurnId ||
          event.commandId === null ||
          !event.commandId.startsWith(`provider:${correlation.runtimeEventId}:`) ||
          event.causationEventId !== null ||
          event.correlationId !== event.commandId ||
          (identity.sealedAtStreamVersion !== undefined &&
            row.streamVersion > identity.sealedAtStreamVersion)
        ) {
          return yield* historyError("result-source-message-identity", "authority-conflict");
        }
        if (
          capture === undefined ||
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
            return yield* historyError("result-source-untagged-message", "authority-conflict");
          }
          continue;
        }
        if (capture.disposition !== "authority") {
          return yield* historyError("result-source-fragment-authority", "authority-conflict");
        }
        if (correlation.runtimeEventId === options?.beforeRuntimeEventId) {
          return currentSnapshot();
        }

        const entry = { event, actorKind, streamVersion: row.streamVersion };
        const fragment = event.payload.fragment;
        if (fragment.kind === "delta") {
          if (correlation.eventType !== "content.delta") {
            return yield* historyError("result-source-delta-runtime-event", "authority-conflict");
          }
          if (openMessageId === null) {
            if (selected?.messageId === event.payload.messageId) {
              return yield* historyError(
                "result-source-message-after-completion",
                "authority-conflict",
              );
            }
            openMessageId = event.payload.messageId;
            openStoredByteLength = 0;
            openOutputByteLength = 0;
            openFragmentOrdinal = 0;
            openCumulativeEvidenceDigest = VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS;
            selected = null;
          } else if (openMessageId !== event.payload.messageId) {
            return yield* historyError("result-source-interleaved-message", "authority-conflict");
          }
          const expectedCumulative = saturatingResultByteLength(
            openOutputByteLength,
            fragment.fullTextByteLength,
          );
          const textBytes = new TextEncoder().encode(fragment.textPrefix);
          const remaining = AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES - openStoredByteLength;
          const expectedOrdinal = openFragmentOrdinal + 1;
          const expectedEvidenceDigest = verificationResultOutputEvidenceDigest({
            previousDigest: openCumulativeEvidenceDigest,
            fragmentKind: "delta",
            fragmentOrdinal: expectedOrdinal,
            fullByteLength: fragment.fullTextByteLength,
            fullDigest: fragment.fullTextDigest,
            detailPresent: true,
          });
          if (
            fragment.cumulativeSourceByteLength !== expectedCumulative ||
            fragment.prefixByteLength !== textBytes.byteLength ||
            textBytes.byteLength > fragment.fullTextByteLength ||
            textBytes.byteLength > remaining ||
            fragment.fragmentOrdinal !== expectedOrdinal ||
            fragment.cumulativeEvidenceDigest !== expectedEvidenceDigest ||
            (expectedCumulative <= AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES &&
              textBytes.byteLength !== fragment.fullTextByteLength) ||
            (fragment.fullTextByteLength <= remaining &&
              fragment.fullTextDigest !== verificationResultDeltaTextDigest(fragment.textPrefix)) ||
            (fragment.fullTextByteLength > remaining && remaining - textBytes.byteLength >= 4) ||
            (openOutputByteLength >= AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL &&
              textBytes.byteLength !== 0)
          ) {
            return yield* historyError("result-source-fragment-length", "authority-conflict");
          }
          buffer.set(textBytes, openStoredByteLength);
          openStoredByteLength += textBytes.byteLength;
          openOutputByteLength = fragment.cumulativeSourceByteLength;
          openFragmentOrdinal = fragment.fragmentOrdinal;
          openCumulativeEvidenceDigest = fragment.cumulativeEvidenceDigest;
          continue;
        }
        if (
          correlation.eventType !== "item.completed" &&
          correlation.eventType !== "turn.completed" &&
          correlation.eventType !== "request.opened" &&
          correlation.eventType !== "user-input.requested"
        ) {
          return yield* historyError(
            "result-source-completion-runtime-event",
            "authority-conflict",
          );
        }
        if (openMessageId === null) {
          const selectedMessageId: MessageId | undefined = (selected as ScannedResultMessage | null)
            ?.messageId;
          if (selectedMessageId === event.payload.messageId) {
            return yield* historyError("result-source-duplicate-completion", "authority-conflict");
          }
          openMessageId = event.payload.messageId;
          openStoredByteLength = 0;
          openOutputByteLength = 0;
          openFragmentOrdinal = 0;
          openCumulativeEvidenceDigest = VERIFICATION_RESULT_OUTPUT_EVIDENCE_GENESIS;
        } else if (openMessageId !== event.payload.messageId) {
          return yield* historyError("result-source-interleaved-completion", "authority-conflict");
        }
        const expectedOrdinal = openFragmentOrdinal + 1;
        const expectedEvidenceDigest = verificationResultOutputEvidenceDigest({
          previousDigest: openCumulativeEvidenceDigest,
          fragmentKind: "completion",
          fragmentOrdinal: expectedOrdinal,
          fullByteLength: fragment.completionDetail.present
            ? fragment.completionDetail.fullByteLength
            : null,
          fullDigest: fragment.completionDetail.present
            ? fragment.completionDetail.fullDigest
            : null,
          detailPresent: fragment.completionDetail.present,
        });
        if (
          fragment.fragmentOrdinal !== expectedOrdinal ||
          fragment.cumulativeEvidenceDigest !== expectedEvidenceDigest
        ) {
          return yield* historyError("result-source-completion-evidence", "authority-conflict");
        }
        if (fragment.completionTextPrefix !== null) {
          const completionBytes = new TextEncoder().encode(fragment.completionTextPrefix);
          if (
            openOutputByteLength !== 0 ||
            openStoredByteLength !== 0 ||
            openFragmentOrdinal !== 0 ||
            !fragment.completionDetail.present ||
            completionBytes.byteLength > AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES ||
            completionBytes.byteLength > fragment.completionDetail.fullByteLength ||
            fragment.outputByteLength !==
              saturatingResultByteLength(0, fragment.completionDetail.fullByteLength) ||
            (fragment.completionDetail.fullByteLength <=
              AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES &&
              (completionBytes.byteLength !== fragment.completionDetail.fullByteLength ||
                fragment.completionDetail.fullDigest !==
                  verificationResultCompletionDetailDigest(fragment.completionTextPrefix))) ||
            (fragment.completionDetail.fullByteLength >
              AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES &&
              AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES - completionBytes.byteLength >= 4)
          ) {
            return yield* historyError("result-source-completion-text", "authority-conflict");
          }
          buffer.set(completionBytes, 0);
          openStoredByteLength = completionBytes.byteLength;
          openOutputByteLength = fragment.outputByteLength;
        } else if (
          fragment.outputByteLength !== openOutputByteLength ||
          (openFragmentOrdinal === 0 && fragment.completionDetail.present)
        ) {
          return yield* historyError("result-source-completion-length", "authority-conflict");
        }
        openFragmentOrdinal = fragment.fragmentOrdinal;
        openCumulativeEvidenceDigest = fragment.cumulativeEvidenceDigest;
        selected = {
          messageId: event.payload.messageId,
          bytes: buffer.subarray(0, openStoredByteLength),
          outputByteLength: openOutputByteLength,
          fragmentOrdinal: openFragmentOrdinal,
          cumulativeEvidenceDigest: openCumulativeEvidenceDigest,
          complete: entry,
        };
        openMessageId = null;
      }
      if (rawRows.length < RESULT_SOURCE_PAGE_SIZE) break;
    }

    return currentSnapshot();
  },
);

export const loadVerificationResultCapturedMessage = Effect.fn(
  "loadVerificationResultCapturedMessage",
)(function* (
  sql: SqlClient.SqlClient,
  identity: VerificationResultCaptureIdentity,
  messageId: MessageId,
  options?: {
    readonly beforeRuntimeEventId?: EventId;
  },
) {
  const snapshot = yield* loadVerificationResultCaptureSnapshot(sql, identity, options);
  const message =
    snapshot.open?.messageId === messageId
      ? snapshot.open
      : snapshot.selected?.messageId === messageId
        ? snapshot.selected
        : null;
  if (message === null) return null;
  const text = yield* Effect.try({
    try: () => new TextDecoder("utf-8", { fatal: true }).decode(message.bytes),
    catch: (cause) => historyError("decode-result-source-text", "corrupt-history", cause),
  });
  return {
    text,
    completed: message.complete !== null,
    outputByteLength: message.outputByteLength,
    storedByteLength: message.bytes.byteLength,
    fragmentOrdinal: message.fragmentOrdinal,
    cumulativeEvidenceDigest: message.cumulativeEvidenceDigest,
  };
});

export const loadOpenVerificationResultMessageIds = Effect.fn(
  "loadOpenVerificationResultMessageIds",
)(function* (sql: SqlClient.SqlClient, identity: VerificationResultCaptureIdentity) {
  const snapshot = yield* loadVerificationResultCaptureSnapshot(sql, identity);
  return snapshot.open === null ? [] : [snapshot.open.messageId];
});

export const loadSealableVerificationResultSource = Effect.fn(
  "loadSealableVerificationResultSource",
)(function* (sql: SqlClient.SqlClient, identity: VerificationResultCaptureIdentity) {
  const snapshot = yield* loadVerificationResultCaptureSnapshot(sql, identity);
  if (snapshot.selected === null) {
    if (snapshot.open !== null) {
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
  if (snapshot.open !== null) {
    return yield* historyError("result-source-later-open-message", "authority-conflict");
  }
  const selected = snapshot.selected;
  const complete = selected.complete!;
  const oversize =
    selected.outputByteLength === AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL;
  if (!oversize && selected.bytes.byteLength !== selected.outputByteLength) {
    return yield* historyError("result-source-captured-length", "authority-conflict");
  }
  return {
    sourceDisposition: oversize ? "oversize" : "captured",
    finalMessageId: selected.messageId,
    bytes: oversize ? new Uint8Array() : selected.bytes,
    outputDigest: oversize ? null : sha256Bytes(selected.bytes),
    outputByteLength: selected.outputByteLength,
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
      FROM main.agent_control_verification_turn_accepted accepted
      JOIN main.agent_control_verification_stage_started_evidence evidence
        ON CAST(evidence.handoff_id AS BLOB) = CAST(accepted.handoff_id AS BLOB)
      JOIN main.agent_control_verification_stage_started_markers started
        ON CAST(started.start_evidence_id AS BLOB) = CAST(evidence.start_evidence_id AS BLOB)
      JOIN main.agent_control_stage_run_states stage
        ON CAST(stage.stage_run_id AS BLOB) = CAST(evidence.stage_run_id AS BLOB)
      JOIN main.agent_control_stage_run_lease_states lease
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
    seal.sourceEventId !== source.sourceEventId ||
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
