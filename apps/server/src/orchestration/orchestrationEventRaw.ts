import { OrchestrationActorKind, OrchestrationEvent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseJsonStrict,
  type JsonValue,
} from "../agentControl/initialPlanning/eventEvidence.ts";
import { decodePersistedOrchestrationMetadata } from "./providerRuntimeMessageCorrelation.ts";

export class OrchestrationEventRawHistoryError extends Schema.TaggedErrorClass<OrchestrationEventRawHistoryError>()(
  "OrchestrationEventRawHistoryError",
  {
    operation: Schema.String,
    reason: Schema.Literals(["persistence", "corrupt-history"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface DecodedOrchestrationEventRow {
  readonly event: OrchestrationEvent;
  readonly actorKind: typeof OrchestrationActorKind.Type;
  readonly streamVersion: number;
  readonly payloadSource: string;
  readonly metadataSource: string;
}

interface RawOrchestrationEventRow extends Record<string, unknown> {
  readonly sequenceStorageClass: unknown;
  readonly sequence: unknown;
  readonly streamVersionStorageClass: unknown;
  readonly streamVersion: unknown;
  readonly eventIdStorageClass: unknown;
  readonly eventIdText: unknown;
  readonly eventIdBytes: unknown;
  readonly aggregateKindStorageClass: unknown;
  readonly aggregateKindText: unknown;
  readonly aggregateKindBytes: unknown;
  readonly aggregateIdStorageClass: unknown;
  readonly aggregateIdText: unknown;
  readonly aggregateIdBytes: unknown;
  readonly eventTypeStorageClass: unknown;
  readonly eventTypeText: unknown;
  readonly eventTypeBytes: unknown;
  readonly occurredAtStorageClass: unknown;
  readonly occurredAtText: unknown;
  readonly occurredAtBytes: unknown;
  readonly commandIdStorageClass: unknown;
  readonly commandIdText: unknown;
  readonly commandIdBytes: unknown;
  readonly causationEventIdStorageClass: unknown;
  readonly causationEventIdText: unknown;
  readonly causationEventIdBytes: unknown;
  readonly correlationIdStorageClass: unknown;
  readonly correlationIdText: unknown;
  readonly correlationIdBytes: unknown;
  readonly actorKindStorageClass: unknown;
  readonly actorKindText: unknown;
  readonly actorKindBytes: unknown;
  readonly payloadStorageClass: unknown;
  readonly payloadText: unknown;
  readonly payloadBytes: unknown;
  readonly metadataStorageClass: unknown;
  readonly metadataText: unknown;
  readonly metadataBytes: unknown;
}

const RAW_EVENT_PAGE_SIZE = 32;
const decodeOrchestrationActorKind = Schema.decodeUnknownEffect(OrchestrationActorKind);
const decodeOrchestrationEvent = Schema.decodeUnknownEffect(OrchestrationEvent);
const encodeOrchestrationActorKind = Schema.encodeUnknownEffect(OrchestrationActorKind);
const encodeOrchestrationEvent = Schema.encodeUnknownEffect(OrchestrationEvent);

const rawError = (
  operation: string,
  reason: OrchestrationEventRawHistoryError["reason"],
  cause?: unknown,
) =>
  new OrchestrationEventRawHistoryError({
    operation,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const routingBytes = (value: string): Uint8Array => new TextEncoder().encode(value);

const containsNul = (value: JsonValue): boolean => {
  if (typeof value === "string") return value.includes("\0");
  if (Array.isArray(value)) return value.some(containsNul);
  if (value === null || typeof value !== "object") return false;
  return Object.entries(value).some(([key, child]) => key.includes("\0") || containsNul(child));
};

const isStoredIsoDateTime = (value: string): boolean => {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(value)) return false;
  return Option.exists(DateTime.make(value), (parsed) => DateTime.formatIso(parsed) === value);
};

const decodeRequiredText = Effect.fn("decodeRequiredOrchestrationText")(function* (input: {
  readonly storageClass: unknown;
  readonly text: unknown;
  readonly bytes: unknown;
  readonly operation: string;
}) {
  if (input.storageClass !== "text" || typeof input.text !== "string") {
    return yield* rawError(`${input.operation}-storage`, "corrupt-history");
  }
  const source = yield* Effect.try({
    try: () => decodeCanonicalUtf8Bytes(input.bytes),
    catch: (cause) => rawError(`${input.operation}-bytes`, "corrupt-history", cause),
  });
  if (source !== input.text) {
    return yield* rawError(`${input.operation}-text-blob-mismatch`, "corrupt-history");
  }
  if (source.includes("\0")) {
    return yield* rawError(`${input.operation}-nul`, "corrupt-history");
  }
  return source;
});

const decodeNullableText = Effect.fn("decodeNullableOrchestrationText")(function* (input: {
  readonly storageClass: unknown;
  readonly text: unknown;
  readonly bytes: unknown;
  readonly operation: string;
}) {
  if (input.storageClass === "null") {
    if (input.text !== null || input.bytes !== null) {
      return yield* rawError(`${input.operation}-null-representation`, "corrupt-history");
    }
    return null;
  }
  return yield* decodeRequiredText(input);
});

const decodeRawOrchestrationEventRow = Effect.fn("decodeRawOrchestrationEventRow")(function* (
  row: RawOrchestrationEventRow,
  operationPrefix: string,
) {
  if (
    row.sequenceStorageClass !== "integer" ||
    typeof row.sequence !== "number" ||
    !Number.isSafeInteger(row.sequence) ||
    row.sequence < 1 ||
    row.streamVersionStorageClass !== "integer" ||
    typeof row.streamVersion !== "number" ||
    !Number.isSafeInteger(row.streamVersion) ||
    row.streamVersion < 0
  ) {
    return yield* rawError(`${operationPrefix}-history-order`, "corrupt-history");
  }

  const [
    eventId,
    aggregateKind,
    aggregateId,
    type,
    occurredAt,
    commandId,
    causationEventId,
    correlationId,
    actorKindText,
    payloadSource,
  ] = yield* Effect.all(
    [
      decodeRequiredText({
        storageClass: row.eventIdStorageClass,
        text: row.eventIdText,
        bytes: row.eventIdBytes,
        operation: `${operationPrefix}-event-id`,
      }),
      decodeRequiredText({
        storageClass: row.aggregateKindStorageClass,
        text: row.aggregateKindText,
        bytes: row.aggregateKindBytes,
        operation: `${operationPrefix}-aggregate-kind`,
      }),
      decodeRequiredText({
        storageClass: row.aggregateIdStorageClass,
        text: row.aggregateIdText,
        bytes: row.aggregateIdBytes,
        operation: `${operationPrefix}-aggregate-id`,
      }),
      decodeRequiredText({
        storageClass: row.eventTypeStorageClass,
        text: row.eventTypeText,
        bytes: row.eventTypeBytes,
        operation: `${operationPrefix}-event-type`,
      }),
      decodeRequiredText({
        storageClass: row.occurredAtStorageClass,
        text: row.occurredAtText,
        bytes: row.occurredAtBytes,
        operation: `${operationPrefix}-occurred-at`,
      }),
      decodeNullableText({
        storageClass: row.commandIdStorageClass,
        text: row.commandIdText,
        bytes: row.commandIdBytes,
        operation: `${operationPrefix}-command-id`,
      }),
      decodeNullableText({
        storageClass: row.causationEventIdStorageClass,
        text: row.causationEventIdText,
        bytes: row.causationEventIdBytes,
        operation: `${operationPrefix}-causation-event-id`,
      }),
      decodeNullableText({
        storageClass: row.correlationIdStorageClass,
        text: row.correlationIdText,
        bytes: row.correlationIdBytes,
        operation: `${operationPrefix}-correlation-id`,
      }),
      decodeRequiredText({
        storageClass: row.actorKindStorageClass,
        text: row.actorKindText,
        bytes: row.actorKindBytes,
        operation: `${operationPrefix}-actor-kind`,
      }),
      decodeRequiredText({
        storageClass: row.payloadStorageClass,
        text: row.payloadText,
        bytes: row.payloadBytes,
        operation: `${operationPrefix}-payload`,
      }),
    ],
    { concurrency: "unbounded" },
  );

  const payload = yield* Effect.try({
    try: () => parseJsonStrict(payloadSource),
    catch: (cause) => rawError(`${operationPrefix}-payload-json`, "corrupt-history", cause),
  });
  if (containsNul(payload)) {
    return yield* rawError(`${operationPrefix}-payload-nul`, "corrupt-history");
  }
  if (row.metadataStorageClass !== "text" || typeof row.metadataText !== "string") {
    return yield* rawError(`${operationPrefix}-metadata-storage`, "corrupt-history");
  }
  const decodedMetadata = yield* Effect.try({
    try: () =>
      decodePersistedOrchestrationMetadata({
        storageClass: row.metadataStorageClass,
        text: row.metadataText,
        bytes: row.metadataBytes,
      }),
    catch: (cause) => rawError(`${operationPrefix}-metadata`, "corrupt-history", cause),
  });
  const metadata = decodedMetadata.value;
  const actorKind = yield* decodeOrchestrationActorKind(actorKindText).pipe(
    Effect.mapError((cause) =>
      rawError(`${operationPrefix}-decode-actor-kind`, "corrupt-history", cause),
    ),
  );
  const encodedActorKind = yield* encodeOrchestrationActorKind(actorKind).pipe(
    Effect.mapError((cause) =>
      rawError(`${operationPrefix}-encode-actor-kind`, "corrupt-history", cause),
    ),
  );
  if (encodedActorKind !== actorKindText) {
    return yield* rawError(`${operationPrefix}-actor-kind-transformed`, "corrupt-history");
  }
  if (!isStoredIsoDateTime(occurredAt)) {
    return yield* rawError(`${operationPrefix}-occurred-at-invalid`, "corrupt-history");
  }
  const event = yield* decodeOrchestrationEvent({
    sequence: row.sequence,
    eventId,
    aggregateKind,
    aggregateId,
    type,
    occurredAt,
    commandId,
    causationEventId,
    correlationId,
    payload,
    metadata,
  }).pipe(
    Effect.mapError((cause) =>
      rawError(`${operationPrefix}-decode-event`, "corrupt-history", cause),
    ),
  );
  const encodedEvent = yield* encodeOrchestrationEvent(event).pipe(
    Effect.mapError((cause) =>
      rawError(`${operationPrefix}-encode-event`, "corrupt-history", cause),
    ),
  );
  const transformedField = [
    ["sequence", encodedEvent.sequence, row.sequence],
    ["event-id", encodedEvent.eventId, eventId],
    ["aggregate-kind", encodedEvent.aggregateKind, aggregateKind],
    ["aggregate-id", encodedEvent.aggregateId, aggregateId],
    ["event-type", encodedEvent.type, type],
    ["occurred-at", encodedEvent.occurredAt, occurredAt],
    ["command-id", encodedEvent.commandId, commandId],
    ["causation-event-id", encodedEvent.causationEventId, causationEventId],
    ["correlation-id", encodedEvent.correlationId, correlationId],
    ["payload", canonicalJson(encodedEvent.payload as JsonValue), payloadSource],
    [
      "metadata",
      canonicalJson(encodedEvent.metadata as JsonValue),
      canonicalJson(metadata as JsonValue),
    ],
  ].find(([, encoded, stored]) => encoded !== stored)?.[0];
  if (transformedField !== undefined) {
    return yield* rawError(
      `${operationPrefix}-event-${transformedField}-transformed`,
      "corrupt-history",
    );
  }
  return {
    event,
    actorKind,
    streamVersion: row.streamVersion,
    payloadSource,
    metadataSource: decodedMetadata.source,
  } satisfies DecodedOrchestrationEventRow;
});

const decodeRows = Effect.fn("decodeRawOrchestrationEventRows")(function* (
  rows: ReadonlyArray<Record<string, unknown>>,
  operationPrefix: string,
) {
  return yield* Effect.forEach(
    rows,
    (row) => decodeRawOrchestrationEventRow(row as RawOrchestrationEventRow, operationPrefix),
    { concurrency: 1 },
  );
});

export const loadOrchestrationEventStreamPage = Effect.fn("loadOrchestrationEventStreamPage")(
  function* (
    sql: SqlClient.SqlClient,
    input: {
      readonly aggregateKind: string;
      readonly aggregateId: string;
      readonly sequenceExclusive: number;
      readonly previousSequence: number;
      readonly previousStreamVersion: number;
      readonly allowZeroInitialStreamVersion?: boolean;
      readonly operationPrefix: string;
    },
  ) {
    const aggregateKindBytes = routingBytes(input.aggregateKind);
    const aggregateIdBytes = routingBytes(input.aggregateId);
    const rawRows = yield* sql<Record<string, unknown>>`
    SELECT typeof(sequence) AS "sequenceStorageClass", sequence,
      typeof(stream_version) AS "streamVersionStorageClass",
      stream_version AS "streamVersion",
      typeof(event_id) AS "eventIdStorageClass", event_id AS "eventIdText",
      CAST(event_id AS BLOB) AS "eventIdBytes",
      typeof(aggregate_kind) AS "aggregateKindStorageClass",
      aggregate_kind AS "aggregateKindText", CAST(aggregate_kind AS BLOB) AS "aggregateKindBytes",
      typeof(stream_id) AS "aggregateIdStorageClass", stream_id AS "aggregateIdText",
      CAST(stream_id AS BLOB) AS "aggregateIdBytes",
      typeof(event_type) AS "eventTypeStorageClass", event_type AS "eventTypeText",
      CAST(event_type AS BLOB) AS "eventTypeBytes",
      typeof(occurred_at) AS "occurredAtStorageClass", occurred_at AS "occurredAtText",
      CAST(occurred_at AS BLOB) AS "occurredAtBytes",
      typeof(command_id) AS "commandIdStorageClass", command_id AS "commandIdText",
      CASE WHEN command_id IS NULL THEN NULL ELSE CAST(command_id AS BLOB) END AS "commandIdBytes",
      typeof(causation_event_id) AS "causationEventIdStorageClass",
      causation_event_id AS "causationEventIdText",
      CASE WHEN causation_event_id IS NULL THEN NULL ELSE CAST(causation_event_id AS BLOB) END
        AS "causationEventIdBytes",
      typeof(correlation_id) AS "correlationIdStorageClass",
      correlation_id AS "correlationIdText",
      CASE WHEN correlation_id IS NULL THEN NULL ELSE CAST(correlation_id AS BLOB) END
        AS "correlationIdBytes",
      typeof(actor_kind) AS "actorKindStorageClass", actor_kind AS "actorKindText",
      CAST(actor_kind AS BLOB) AS "actorKindBytes",
      typeof(payload_json) AS "payloadStorageClass", payload_json AS "payloadText",
      CAST(payload_json AS BLOB) AS "payloadBytes",
      typeof(metadata_json) AS "metadataStorageClass", metadata_json AS "metadataText",
      CAST(metadata_json AS BLOB) AS "metadataBytes"
    FROM main.orchestration_events
    WHERE sequence > ${input.sequenceExclusive}
      AND CAST(aggregate_kind AS BLOB) = ${aggregateKindBytes}
      AND CAST(stream_id AS BLOB) = ${aggregateIdBytes}
    ORDER BY sequence
    LIMIT ${RAW_EVENT_PAGE_SIZE}
  `.pipe(
      Effect.mapError((cause) =>
        rawError(`${input.operationPrefix}-read-history`, "persistence", cause),
      ),
    );
    const rows = yield* decodeRows(rawRows, input.operationPrefix);
    let previousSequence = input.previousSequence;
    let previousStreamVersion = input.previousStreamVersion;
    for (const row of rows) {
      if (
        row.event.aggregateKind !== input.aggregateKind ||
        row.event.aggregateId !== input.aggregateId ||
        row.event.sequence <= previousSequence ||
        (input.allowZeroInitialStreamVersion && previousSequence === 0
          ? row.streamVersion !== 0 && row.streamVersion !== 1
          : row.streamVersion !== previousStreamVersion + 1)
      ) {
        return yield* rawError(`${input.operationPrefix}-history-order`, "corrupt-history");
      }
      previousSequence = row.event.sequence;
      previousStreamVersion = row.streamVersion;
    }
    return {
      rows,
      nextSequenceExclusive: previousSequence,
      nextStreamVersion: previousStreamVersion,
    };
  },
);

export const loadOrchestrationEventsByTypePage = Effect.fn("loadOrchestrationEventsByTypePage")(
  function* (
    sql: SqlClient.SqlClient,
    input: {
      readonly aggregateKind: string;
      readonly eventType: string;
      readonly sequenceExclusive: number;
      readonly operationPrefix: string;
    },
  ) {
    const aggregateKindBytes = routingBytes(input.aggregateKind);
    const eventTypeBytes = routingBytes(input.eventType);
    const rawRows = yield* sql<Record<string, unknown>>`
    SELECT typeof(sequence) AS "sequenceStorageClass", sequence,
      typeof(stream_version) AS "streamVersionStorageClass",
      stream_version AS "streamVersion",
      typeof(event_id) AS "eventIdStorageClass", event_id AS "eventIdText",
      CAST(event_id AS BLOB) AS "eventIdBytes",
      typeof(aggregate_kind) AS "aggregateKindStorageClass",
      aggregate_kind AS "aggregateKindText", CAST(aggregate_kind AS BLOB) AS "aggregateKindBytes",
      typeof(stream_id) AS "aggregateIdStorageClass", stream_id AS "aggregateIdText",
      CAST(stream_id AS BLOB) AS "aggregateIdBytes",
      typeof(event_type) AS "eventTypeStorageClass", event_type AS "eventTypeText",
      CAST(event_type AS BLOB) AS "eventTypeBytes",
      typeof(occurred_at) AS "occurredAtStorageClass", occurred_at AS "occurredAtText",
      CAST(occurred_at AS BLOB) AS "occurredAtBytes",
      typeof(command_id) AS "commandIdStorageClass", command_id AS "commandIdText",
      CASE WHEN command_id IS NULL THEN NULL ELSE CAST(command_id AS BLOB) END AS "commandIdBytes",
      typeof(causation_event_id) AS "causationEventIdStorageClass",
      causation_event_id AS "causationEventIdText",
      CASE WHEN causation_event_id IS NULL THEN NULL ELSE CAST(causation_event_id AS BLOB) END
        AS "causationEventIdBytes",
      typeof(correlation_id) AS "correlationIdStorageClass",
      correlation_id AS "correlationIdText",
      CASE WHEN correlation_id IS NULL THEN NULL ELSE CAST(correlation_id AS BLOB) END
        AS "correlationIdBytes",
      typeof(actor_kind) AS "actorKindStorageClass", actor_kind AS "actorKindText",
      CAST(actor_kind AS BLOB) AS "actorKindBytes",
      typeof(payload_json) AS "payloadStorageClass", payload_json AS "payloadText",
      CAST(payload_json AS BLOB) AS "payloadBytes",
      typeof(metadata_json) AS "metadataStorageClass", metadata_json AS "metadataText",
      CAST(metadata_json AS BLOB) AS "metadataBytes"
    FROM main.orchestration_events
    WHERE sequence > ${input.sequenceExclusive}
      AND CAST(aggregate_kind AS BLOB) = ${aggregateKindBytes}
      AND CAST(event_type AS BLOB) = ${eventTypeBytes}
    ORDER BY sequence
    LIMIT ${RAW_EVENT_PAGE_SIZE}
  `.pipe(
      Effect.mapError((cause) =>
        rawError(`${input.operationPrefix}-read-history`, "persistence", cause),
      ),
    );
    const rows = yield* decodeRows(rawRows, input.operationPrefix);
    for (const row of rows) {
      if (row.event.aggregateKind !== input.aggregateKind || row.event.type !== input.eventType) {
        return yield* rawError(`${input.operationPrefix}-routing`, "corrupt-history");
      }
    }
    return {
      rows,
      nextSequenceExclusive: rows.at(-1)?.event.sequence ?? input.sequenceExclusive,
    };
  },
);

export const loadOrchestrationEventsByCommandIdPage = Effect.fn(
  "loadOrchestrationEventsByCommandIdPage",
)(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly commandId: string;
    readonly sequenceExclusive: number;
    readonly operationPrefix: string;
  },
) {
  const commandIdBytes = routingBytes(input.commandId);
  const rawRows = yield* sql<Record<string, unknown>>`
    SELECT typeof(sequence) AS "sequenceStorageClass", sequence,
      typeof(stream_version) AS "streamVersionStorageClass",
      stream_version AS "streamVersion",
      typeof(event_id) AS "eventIdStorageClass", event_id AS "eventIdText",
      CAST(event_id AS BLOB) AS "eventIdBytes",
      typeof(aggregate_kind) AS "aggregateKindStorageClass",
      aggregate_kind AS "aggregateKindText", CAST(aggregate_kind AS BLOB) AS "aggregateKindBytes",
      typeof(stream_id) AS "aggregateIdStorageClass", stream_id AS "aggregateIdText",
      CAST(stream_id AS BLOB) AS "aggregateIdBytes",
      typeof(event_type) AS "eventTypeStorageClass", event_type AS "eventTypeText",
      CAST(event_type AS BLOB) AS "eventTypeBytes",
      typeof(occurred_at) AS "occurredAtStorageClass", occurred_at AS "occurredAtText",
      CAST(occurred_at AS BLOB) AS "occurredAtBytes",
      typeof(command_id) AS "commandIdStorageClass", command_id AS "commandIdText",
      CASE WHEN command_id IS NULL THEN NULL ELSE CAST(command_id AS BLOB) END AS "commandIdBytes",
      typeof(causation_event_id) AS "causationEventIdStorageClass",
      causation_event_id AS "causationEventIdText",
      CASE WHEN causation_event_id IS NULL THEN NULL ELSE CAST(causation_event_id AS BLOB) END
        AS "causationEventIdBytes",
      typeof(correlation_id) AS "correlationIdStorageClass",
      correlation_id AS "correlationIdText",
      CASE WHEN correlation_id IS NULL THEN NULL ELSE CAST(correlation_id AS BLOB) END
        AS "correlationIdBytes",
      typeof(actor_kind) AS "actorKindStorageClass", actor_kind AS "actorKindText",
      CAST(actor_kind AS BLOB) AS "actorKindBytes",
      typeof(payload_json) AS "payloadStorageClass", payload_json AS "payloadText",
      CAST(payload_json AS BLOB) AS "payloadBytes",
      typeof(metadata_json) AS "metadataStorageClass", metadata_json AS "metadataText",
      CAST(metadata_json AS BLOB) AS "metadataBytes"
    FROM main.orchestration_events
    WHERE sequence > ${input.sequenceExclusive}
      AND CAST(command_id AS BLOB) = ${commandIdBytes}
    ORDER BY sequence
    LIMIT ${RAW_EVENT_PAGE_SIZE}
  `.pipe(
    Effect.mapError((cause) =>
      rawError(`${input.operationPrefix}-read-history`, "persistence", cause),
    ),
  );
  const rows = yield* decodeRows(rawRows, input.operationPrefix);
  let previousSequence = input.sequenceExclusive;
  for (const row of rows) {
    if (row.event.commandId !== input.commandId || row.event.sequence <= previousSequence) {
      return yield* rawError(`${input.operationPrefix}-command-routing`, "corrupt-history");
    }
    previousSequence = row.event.sequence;
  }
  return {
    rows,
    nextSequenceExclusive: previousSequence,
  };
});

export const loadOrchestrationEventBySequence = Effect.fn("loadOrchestrationEventBySequence")(
  function* (
    sql: SqlClient.SqlClient,
    input: {
      readonly sequence: number;
      readonly operationPrefix: string;
    },
  ) {
    const rawRows = yield* sql<Record<string, unknown>>`
    SELECT typeof(sequence) AS "sequenceStorageClass", sequence,
      typeof(stream_version) AS "streamVersionStorageClass",
      stream_version AS "streamVersion",
      typeof(event_id) AS "eventIdStorageClass", event_id AS "eventIdText",
      CAST(event_id AS BLOB) AS "eventIdBytes",
      typeof(aggregate_kind) AS "aggregateKindStorageClass",
      aggregate_kind AS "aggregateKindText", CAST(aggregate_kind AS BLOB) AS "aggregateKindBytes",
      typeof(stream_id) AS "aggregateIdStorageClass", stream_id AS "aggregateIdText",
      CAST(stream_id AS BLOB) AS "aggregateIdBytes",
      typeof(event_type) AS "eventTypeStorageClass", event_type AS "eventTypeText",
      CAST(event_type AS BLOB) AS "eventTypeBytes",
      typeof(occurred_at) AS "occurredAtStorageClass", occurred_at AS "occurredAtText",
      CAST(occurred_at AS BLOB) AS "occurredAtBytes",
      typeof(command_id) AS "commandIdStorageClass", command_id AS "commandIdText",
      CASE WHEN command_id IS NULL THEN NULL ELSE CAST(command_id AS BLOB) END AS "commandIdBytes",
      typeof(causation_event_id) AS "causationEventIdStorageClass",
      causation_event_id AS "causationEventIdText",
      CASE WHEN causation_event_id IS NULL THEN NULL ELSE CAST(causation_event_id AS BLOB) END
        AS "causationEventIdBytes",
      typeof(correlation_id) AS "correlationIdStorageClass",
      correlation_id AS "correlationIdText",
      CASE WHEN correlation_id IS NULL THEN NULL ELSE CAST(correlation_id AS BLOB) END
        AS "correlationIdBytes",
      typeof(actor_kind) AS "actorKindStorageClass", actor_kind AS "actorKindText",
      CAST(actor_kind AS BLOB) AS "actorKindBytes",
      typeof(payload_json) AS "payloadStorageClass", payload_json AS "payloadText",
      CAST(payload_json AS BLOB) AS "payloadBytes",
      typeof(metadata_json) AS "metadataStorageClass", metadata_json AS "metadataText",
      CAST(metadata_json AS BLOB) AS "metadataBytes"
    FROM main.orchestration_events
    WHERE sequence = ${input.sequence}
  `.pipe(
      Effect.mapError((cause) =>
        rawError(`${input.operationPrefix}-read-history`, "persistence", cause),
      ),
    );
    if (rawRows.length > 1) {
      return yield* rawError(`${input.operationPrefix}-sequence-count`, "corrupt-history");
    }
    const rows = yield* decodeRows(rawRows, input.operationPrefix);
    return rows[0] ?? null;
  },
);
