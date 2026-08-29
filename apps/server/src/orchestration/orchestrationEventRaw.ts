import { OrchestrationActorKind, OrchestrationEvent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  type JsonValue,
} from "../agentControl/initialPlanning/eventEvidence.ts";
import {
  decodeOrchestrationEventJsonStorage,
  orchestrationEventAuthorityRouteBytes,
  orchestrationEventProjectMembershipRouteBytes,
  ORCHESTRATION_EVENT_ROUTE_INVALID,
} from "./orchestrationEventStorage.ts";
import {
  SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION,
  SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
} from "../persistence/SqliteFunctions.ts";

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

const rawEventColumns = (row: string): string => `
  typeof(${row}.sequence) AS "sequenceStorageClass", ${row}.sequence,
  typeof(${row}.stream_version) AS "streamVersionStorageClass",
  ${row}.stream_version AS "streamVersion",
  typeof(${row}.event_id) AS "eventIdStorageClass", ${row}.event_id AS "eventIdText",
  CAST(${row}.event_id AS BLOB) AS "eventIdBytes",
  typeof(${row}.aggregate_kind) AS "aggregateKindStorageClass",
  ${row}.aggregate_kind AS "aggregateKindText",
  CAST(${row}.aggregate_kind AS BLOB) AS "aggregateKindBytes",
  typeof(${row}.stream_id) AS "aggregateIdStorageClass", ${row}.stream_id AS "aggregateIdText",
  CAST(${row}.stream_id AS BLOB) AS "aggregateIdBytes",
  typeof(${row}.event_type) AS "eventTypeStorageClass", ${row}.event_type AS "eventTypeText",
  CAST(${row}.event_type AS BLOB) AS "eventTypeBytes",
  typeof(${row}.occurred_at) AS "occurredAtStorageClass", ${row}.occurred_at AS "occurredAtText",
  CAST(${row}.occurred_at AS BLOB) AS "occurredAtBytes",
  typeof(${row}.command_id) AS "commandIdStorageClass", ${row}.command_id AS "commandIdText",
  CASE WHEN ${row}.command_id IS NULL THEN NULL ELSE CAST(${row}.command_id AS BLOB) END
    AS "commandIdBytes",
  typeof(${row}.causation_event_id) AS "causationEventIdStorageClass",
  ${row}.causation_event_id AS "causationEventIdText",
  CASE WHEN ${row}.causation_event_id IS NULL THEN NULL
    ELSE CAST(${row}.causation_event_id AS BLOB) END AS "causationEventIdBytes",
  typeof(${row}.correlation_id) AS "correlationIdStorageClass",
  ${row}.correlation_id AS "correlationIdText",
  CASE WHEN ${row}.correlation_id IS NULL THEN NULL ELSE CAST(${row}.correlation_id AS BLOB) END
    AS "correlationIdBytes",
  typeof(${row}.actor_kind) AS "actorKindStorageClass", ${row}.actor_kind AS "actorKindText",
  CAST(${row}.actor_kind AS BLOB) AS "actorKindBytes",
  typeof(${row}.payload_json) AS "payloadStorageClass", ${row}.payload_json AS "payloadText",
  CAST(${row}.payload_json AS BLOB) AS "payloadBytes",
  typeof(${row}.metadata_json) AS "metadataStorageClass", ${row}.metadata_json AS "metadataText",
  CAST(${row}.metadata_json AS BLOB) AS "metadataBytes"`;

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

const eventAuthorityRoute = (
  event: OrchestrationEvent,
): { readonly aggregateKind: "project" | "thread"; readonly aggregateId: string } => {
  const aggregateKind = event.type.startsWith("project.") ? "project" : "thread";
  const payload = event.payload as { readonly projectId?: unknown; readonly threadId?: unknown };
  const aggregateId = aggregateKind === "project" ? payload.projectId : payload.threadId;
  if (typeof aggregateId !== "string") {
    throw new Error("Orchestration payload has no authority routing identifier");
  }
  return { aggregateKind, aggregateId };
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

  const decodedStorage = yield* Effect.try({
    try: () =>
      decodeOrchestrationEventJsonStorage({
        eventType: type,
        payload: {
          storageClass: row.payloadStorageClass,
          text: row.payloadText,
          bytes: row.payloadBytes,
        },
        metadata: {
          storageClass: row.metadataStorageClass,
          text: row.metadataText,
          bytes: row.metadataBytes,
        },
      }),
    catch: (cause) => rawError(`${operationPrefix}-json-storage`, "corrupt-history", cause),
  });
  const payload = decodedStorage.payload;
  const metadata = decodedStorage.metadata;
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
  const authorityRoute = yield* Effect.try({
    try: () => eventAuthorityRoute(event),
    catch: (cause) => rawError(`${operationPrefix}-routing-authority`, "corrupt-history", cause),
  });
  if (
    event.aggregateKind !== authorityRoute.aggregateKind ||
    event.aggregateId !== authorityRoute.aggregateId
  ) {
    return yield* rawError(`${operationPrefix}-routing-authority`, "corrupt-history");
  }
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
    metadataSource: decodedStorage.metadataSource,
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

const mergeDecodedAuthorityRows = (
  branches: ReadonlyArray<ReadonlyArray<DecodedOrchestrationEventRow>>,
): ReadonlyArray<DecodedOrchestrationEventRow> => {
  const bySequence = new Map<number, DecodedOrchestrationEventRow>();
  for (const branch of branches) {
    for (const row of branch) bySequence.set(row.event.sequence, row);
  }
  return [...bySequence.values()]
    .toSorted((left, right) => left.event.sequence - right.event.sequence)
    .slice(0, RAW_EVENT_PAGE_SIZE);
};

const mergeRawAuthorityBranches = Effect.fn("mergeRawOrchestrationAuthorityBranches")(function* (
  branches: ReadonlyArray<{
    readonly name: string;
    readonly rows: ReadonlyArray<Record<string, unknown>>;
  }>,
  operationPrefix: string,
) {
  const decodedBranches: Array<ReadonlyArray<DecodedOrchestrationEventRow>> = [];
  for (const branch of branches) {
    const decoded = yield* decodeRows(branch.rows, `${operationPrefix}-${branch.name}`);
    decodedBranches.push(decoded);
  }
  return mergeDecodedAuthorityRows(decodedBranches);
});

const invalidRouteClaimPredicate = (property: "projectId" | "threadId"): string => `
  CASE
    WHEN json_valid(event.payload_json) = 1 THEN EXISTS (
      SELECT 1 FROM json_each(event.payload_json) AS route_claim
      WHERE route_claim.key = '${property}'
        AND typeof(route_claim.value) = 'text'
        AND CAST(route_claim.value AS BLOB) = ?
    )
    ELSE 1
  END = 1`;

const loadAuthorityRouteBranches = Effect.fn("loadOrchestrationAuthorityRouteBranches")(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly aggregateKind: "project" | "thread";
    readonly aggregateId: string;
    readonly sequenceExclusive: number;
    readonly sequenceUpperExclusive: number;
    readonly operationPrefix: string;
  },
) {
  const aggregateKindBytes = routingBytes(input.aggregateKind);
  const aggregateIdBytes = routingBytes(input.aggregateId);
  const authorityRouteBytes = orchestrationEventAuthorityRouteBytes(
    input.aggregateKind,
    input.aggregateId,
  );
  const routeProperty = input.aggregateKind === "project" ? "projectId" : "threadId";
  const readBranch = (name: string, query: string, parameters: ReadonlyArray<unknown>) =>
    sql
      .unsafe<Record<string, unknown>>(query, parameters)
      .pipe(
        Effect.mapError((cause) =>
          rawError(`${input.operationPrefix}-read-${name}`, "persistence", cause),
        ),
      );
  const physical = yield* readBranch(
    "physical-route",
    `SELECT ${rawEventColumns("event")}
       FROM main.orchestration_events AS event
       WHERE event.sequence > ? AND event.sequence < ?
         AND CAST(event.aggregate_kind AS BLOB) = ?
         AND CAST(event.stream_id AS BLOB) = ?
       ORDER BY event.sequence
       LIMIT ${RAW_EVENT_PAGE_SIZE}`,
    [input.sequenceExclusive, input.sequenceUpperExclusive, aggregateKindBytes, aggregateIdBytes],
  );
  const claimed = yield* readBranch(
    "claimed-route",
    `SELECT ${rawEventColumns("event")}
       FROM main.orchestration_events AS event
       WHERE event.sequence > ? AND event.sequence < ?
         AND ${SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION}(
           CAST(event.event_type AS BLOB), CAST(event.payload_json AS BLOB),
           CAST(event.metadata_json AS BLOB)
         ) = ?
       ORDER BY event.sequence
       LIMIT ${RAW_EVENT_PAGE_SIZE}`,
    [input.sequenceExclusive, input.sequenceUpperExclusive, authorityRouteBytes],
  );
  const invalid = yield* readBranch(
    "invalid-route",
    `SELECT ${rawEventColumns("event")}
       FROM main.orchestration_events AS event
       WHERE event.sequence > ? AND event.sequence < ?
         AND ${SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION}(
           CAST(event.event_type AS BLOB), CAST(event.payload_json AS BLOB),
           CAST(event.metadata_json AS BLOB)
         ) = ?
         AND ${invalidRouteClaimPredicate(routeProperty)}
       ORDER BY event.sequence
       LIMIT ${RAW_EVENT_PAGE_SIZE}`,
    [
      input.sequenceExclusive,
      input.sequenceUpperExclusive,
      ORCHESTRATION_EVENT_ROUTE_INVALID,
      aggregateIdBytes,
    ],
  );
  return yield* mergeRawAuthorityBranches(
    [
      { name: "physical-route", rows: physical },
      { name: "claimed-route", rows: claimed },
      { name: "invalid-route", rows: invalid },
    ],
    input.operationPrefix,
  );
});

const loadImmediatePredecessors = Effect.fn("loadImmediateOrchestrationPredecessors")(function* (
  sql: SqlClient.SqlClient,
  rows: ReadonlyArray<DecodedOrchestrationEventRow>,
  operationPrefix: string,
  onQuery?: (observation: OrchestrationCommandReplayQueryObservation) => void,
) {
  if (rows.length === 0) return [] as ReadonlyArray<DecodedOrchestrationEventRow | null>;
  const values = rows.map(() => "(?, ?, ?, ?)").join(", ");
  const parameters = rows.flatMap((row, ordinal) => [
    ordinal,
    routingBytes(row.event.aggregateKind),
    routingBytes(row.event.aggregateId),
    row.event.sequence,
  ]);
  const rawRows = yield* sql
    .unsafe<Record<string, unknown>>(
      `WITH targets(target_ordinal, aggregate_kind_bytes, stream_id_bytes, candidate_sequence) AS (
          VALUES ${values}
        )
        SELECT targets.target_ordinal AS "targetOrdinal",
          typeof(prior.sequence) AS "sequenceStorageClass", prior.sequence,
          typeof(prior.stream_version) AS "streamVersionStorageClass",
          prior.stream_version AS "streamVersion",
          typeof(prior.event_id) AS "eventIdStorageClass", prior.event_id AS "eventIdText",
          CAST(prior.event_id AS BLOB) AS "eventIdBytes",
          typeof(prior.aggregate_kind) AS "aggregateKindStorageClass",
          prior.aggregate_kind AS "aggregateKindText",
          CAST(prior.aggregate_kind AS BLOB) AS "aggregateKindBytes",
          typeof(prior.stream_id) AS "aggregateIdStorageClass", prior.stream_id AS "aggregateIdText",
          CAST(prior.stream_id AS BLOB) AS "aggregateIdBytes",
          typeof(prior.event_type) AS "eventTypeStorageClass", prior.event_type AS "eventTypeText",
          CAST(prior.event_type AS BLOB) AS "eventTypeBytes",
          typeof(prior.occurred_at) AS "occurredAtStorageClass", prior.occurred_at AS "occurredAtText",
          CAST(prior.occurred_at AS BLOB) AS "occurredAtBytes",
          typeof(prior.command_id) AS "commandIdStorageClass", prior.command_id AS "commandIdText",
          CASE WHEN prior.command_id IS NULL THEN NULL ELSE CAST(prior.command_id AS BLOB) END
            AS "commandIdBytes",
          typeof(prior.causation_event_id) AS "causationEventIdStorageClass",
          prior.causation_event_id AS "causationEventIdText",
          CASE WHEN prior.causation_event_id IS NULL THEN NULL
            ELSE CAST(prior.causation_event_id AS BLOB) END AS "causationEventIdBytes",
          typeof(prior.correlation_id) AS "correlationIdStorageClass",
          prior.correlation_id AS "correlationIdText",
          CASE WHEN prior.correlation_id IS NULL THEN NULL ELSE CAST(prior.correlation_id AS BLOB) END
            AS "correlationIdBytes",
          typeof(prior.actor_kind) AS "actorKindStorageClass", prior.actor_kind AS "actorKindText",
          CAST(prior.actor_kind AS BLOB) AS "actorKindBytes",
          typeof(prior.payload_json) AS "payloadStorageClass", prior.payload_json AS "payloadText",
          CAST(prior.payload_json AS BLOB) AS "payloadBytes",
          typeof(prior.metadata_json) AS "metadataStorageClass", prior.metadata_json AS "metadataText",
          CAST(prior.metadata_json AS BLOB) AS "metadataBytes"
        FROM targets
        LEFT JOIN main.orchestration_events AS prior ON prior.sequence = (
          SELECT predecessor.sequence
          FROM main.orchestration_events AS predecessor
          WHERE predecessor.sequence < targets.candidate_sequence
            AND CAST(predecessor.aggregate_kind AS BLOB) = targets.aggregate_kind_bytes
            AND CAST(predecessor.stream_id AS BLOB) = targets.stream_id_bytes
          ORDER BY predecessor.sequence DESC
          LIMIT 1
        )
        ORDER BY targets.target_ordinal`,
      parameters,
    )
    .pipe(
      Effect.mapError((cause) =>
        rawError(`${operationPrefix}-read-stream-predecessors`, "persistence", cause),
      ),
    );
  onQuery?.({ kind: "stream-predecessors", rowCount: rawRows.length });
  if (rawRows.length !== rows.length) {
    return yield* rawError(`${operationPrefix}-stream-predecessor-count`, "corrupt-history");
  }
  const predecessors: Array<DecodedOrchestrationEventRow | null> = [];
  for (const [ordinal, rawRow] of rawRows.entries()) {
    if (rawRow.targetOrdinal !== ordinal) {
      return yield* rawError(`${operationPrefix}-stream-predecessor-order`, "corrupt-history");
    }
    if (rawRow.sequenceStorageClass === "null" && rawRow.sequence === null) {
      predecessors.push(null);
      continue;
    }
    predecessors.push(
      yield* decodeRawOrchestrationEventRow(
        rawRow as unknown as RawOrchestrationEventRow,
        `${operationPrefix}-stream-predecessor`,
      ),
    );
  }
  return predecessors;
});

export interface OrchestrationCommandReplayQueryObservation {
  readonly kind:
    | "command-candidates"
    | "stream-predecessors"
    | "project-thread-creations"
    | "thread-authority-streams";
  readonly rowCount: number;
}

export const loadOrchestrationEventsAfterSequencePage = Effect.fn(
  "loadOrchestrationEventsAfterSequencePage",
)(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly sequenceExclusive: number;
    readonly sequenceUpperExclusive: number;
    readonly limit: number;
    readonly operationPrefix: string;
  },
) {
  const pageLimit = Math.max(0, Math.min(RAW_EVENT_PAGE_SIZE, Math.floor(input.limit)));
  if (pageLimit === 0) {
    return {
      rows: [] as ReadonlyArray<DecodedOrchestrationEventRow>,
      nextSequenceExclusive: input.sequenceExclusive,
    };
  }
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
      AND sequence < ${input.sequenceUpperExclusive}
    ORDER BY sequence
    LIMIT ${pageLimit}
  `.pipe(
    Effect.mapError((cause) =>
      rawError(`${input.operationPrefix}-read-history`, "persistence", cause),
    ),
  );
  const rows = yield* decodeRows(rawRows, input.operationPrefix);
  let previousSequence = input.sequenceExclusive;
  for (const row of rows) {
    if (row.event.sequence <= previousSequence) {
      return yield* rawError(`${input.operationPrefix}-history-order`, "corrupt-history");
    }
    previousSequence = row.event.sequence;
  }
  return { rows, nextSequenceExclusive: previousSequence };
});

export const loadOrchestrationEventStreamPage = Effect.fn("loadOrchestrationEventStreamPage")(
  function* (
    sql: SqlClient.SqlClient,
    input: {
      readonly aggregateKind: string;
      readonly aggregateId: string;
      readonly sequenceExclusive: number;
      readonly sequenceUpperExclusive?: number;
      readonly previousSequence: number;
      readonly previousStreamVersion: number;
      readonly operationPrefix: string;
    },
  ) {
    const aggregateKindBytes = routingBytes(input.aggregateKind);
    const aggregateIdBytes = routingBytes(input.aggregateId);
    const sequenceUpperExclusive = input.sequenceUpperExclusive ?? Number.MAX_SAFE_INTEGER + 1;
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
      AND sequence < ${sequenceUpperExclusive}
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
        (previousSequence === 0
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

/**
 * Load the complete authority routing set for one thread. Besides the physical
 * thread stream, this includes any row whose payload claims the target thread,
 * so neither half of a routing mismatch can be hidden by the lookup predicate.
 */
export const loadOrchestrationThreadAuthorityStreamPage = Effect.fn(
  "loadOrchestrationThreadAuthorityStreamPage",
)(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly threadId: string;
    readonly sequenceExclusive: number;
    readonly sequenceUpperExclusive: number;
    readonly previousSequence: number;
    readonly previousStreamVersion: number;
    readonly operationPrefix: string;
  },
) {
  const rows = yield* loadAuthorityRouteBranches(sql, {
    aggregateKind: "thread",
    aggregateId: input.threadId,
    sequenceExclusive: input.sequenceExclusive,
    sequenceUpperExclusive: input.sequenceUpperExclusive,
    operationPrefix: input.operationPrefix,
  });
  let previousSequence = input.previousSequence;
  let previousStreamVersion = input.previousStreamVersion;
  for (const row of rows) {
    if (
      row.event.aggregateKind !== "thread" ||
      row.event.aggregateId !== input.threadId ||
      row.event.sequence <= previousSequence ||
      (previousSequence === 0
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
});

export const loadOrchestrationProjectThreadCreationsPage = Effect.fn(
  "loadOrchestrationProjectThreadCreationsPage",
)(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly projectId: string;
    readonly sequenceExclusive: number;
    readonly sequenceUpperExclusive: number;
    readonly operationPrefix: string;
    readonly onQuery?: (observation: OrchestrationCommandReplayQueryObservation) => void;
  },
) {
  const projectIdBytes = routingBytes(input.projectId);
  const validRows = yield* sql
    .unsafe<Record<string, unknown>>(
      `SELECT ${rawEventColumns("event")}
       FROM main.orchestration_events AS event
       WHERE event.sequence > ? AND event.sequence < ?
         AND ${SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION}(
           CAST(event.event_type AS BLOB), CAST(event.payload_json AS BLOB),
           CAST(event.metadata_json AS BLOB)
         ) = ?
       ORDER BY event.sequence
       LIMIT ${RAW_EVENT_PAGE_SIZE}`,
      [
        input.sequenceExclusive,
        input.sequenceUpperExclusive,
        orchestrationEventProjectMembershipRouteBytes(input.projectId),
      ],
    )
    .pipe(
      Effect.mapError((cause) =>
        rawError(`${input.operationPrefix}-read-thread-creations`, "persistence", cause),
      ),
    );
  const invalidRows = yield* sql
    .unsafe<Record<string, unknown>>(
      `SELECT ${rawEventColumns("event")}
       FROM main.orchestration_events AS event
       WHERE event.sequence > ? AND event.sequence < ?
         AND ${SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION}(
           CAST(event.event_type AS BLOB), CAST(event.payload_json AS BLOB),
           CAST(event.metadata_json AS BLOB)
         ) = ?
         AND ${invalidRouteClaimPredicate("projectId")}
       ORDER BY event.sequence
       LIMIT ${RAW_EVENT_PAGE_SIZE}`,
      [
        input.sequenceExclusive,
        input.sequenceUpperExclusive,
        ORCHESTRATION_EVENT_ROUTE_INVALID,
        projectIdBytes,
      ],
    )
    .pipe(
      Effect.mapError((cause) =>
        rawError(`${input.operationPrefix}-read-invalid-thread-creations`, "persistence", cause),
      ),
    );
  const rows = yield* mergeRawAuthorityBranches(
    [
      { name: "project-membership-route", rows: validRows },
      { name: "invalid-project-membership-route", rows: invalidRows },
    ],
    input.operationPrefix,
  );
  input.onQuery?.({ kind: "project-thread-creations", rowCount: rows.length });
  for (const row of rows) {
    if (
      row.event.type !== "thread.created" ||
      row.event.payload.projectId !== input.projectId ||
      row.event.sequence <= input.sequenceExclusive ||
      row.event.sequence >= input.sequenceUpperExclusive
    ) {
      return yield* rawError(`${input.operationPrefix}-thread-creation-routing`, "corrupt-history");
    }
  }
  return {
    rows,
    nextSequenceExclusive: rows.at(-1)?.event.sequence ?? input.sequenceExclusive,
  };
});

export const loadOrchestrationProjectAuthorityStreamPage = Effect.fn(
  "loadOrchestrationProjectAuthorityStreamPage",
)(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly projectId: string;
    readonly sequenceExclusive: number;
    readonly sequenceUpperExclusive: number;
    readonly previousStreamVersion: number;
    readonly operationPrefix: string;
  },
) {
  const rows = yield* loadAuthorityRouteBranches(sql, {
    aggregateKind: "project",
    aggregateId: input.projectId,
    sequenceExclusive: input.sequenceExclusive,
    sequenceUpperExclusive: input.sequenceUpperExclusive,
    operationPrefix: input.operationPrefix,
  });
  let previousSequence = input.sequenceExclusive;
  let previousStreamVersion = input.previousStreamVersion;
  for (const row of rows) {
    if (
      row.event.aggregateKind !== "project" ||
      row.event.aggregateId !== input.projectId ||
      row.event.sequence <= previousSequence ||
      (previousSequence === 0
        ? row.streamVersion !== 0 && row.streamVersion !== 1
        : row.streamVersion !== previousStreamVersion + 1)
    ) {
      return yield* rawError(`${input.operationPrefix}-project-routing`, "corrupt-history");
    }
    previousSequence = row.event.sequence;
    previousStreamVersion = row.streamVersion;
  }
  return {
    rows,
    nextSequenceExclusive: previousSequence,
    nextStreamVersion: previousStreamVersion,
  };
});

export const loadOrchestrationThreadAuthorityStreamsPage = Effect.fn(
  "loadOrchestrationThreadAuthorityStreamsPage",
)(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly threadIds: ReadonlyArray<string>;
    readonly sequenceExclusive: number;
    readonly sequenceUpperExclusive: number;
    readonly previousStreamVersions: ReadonlyMap<string, number>;
    readonly operationPrefix: string;
    readonly onQuery?: (observation: OrchestrationCommandReplayQueryObservation) => void;
  },
) {
  if (input.threadIds.length < 1 || input.threadIds.length > RAW_EVENT_PAGE_SIZE) {
    return yield* rawError(`${input.operationPrefix}-thread-group-size`, "corrupt-history");
  }
  const branchPages = yield* Effect.forEach(
    input.threadIds,
    (threadId) =>
      loadAuthorityRouteBranches(sql, {
        aggregateKind: "thread",
        aggregateId: threadId,
        sequenceExclusive: input.sequenceExclusive,
        sequenceUpperExclusive: input.sequenceUpperExclusive,
        operationPrefix: input.operationPrefix,
      }),
    { concurrency: 1 },
  );
  const rows = mergeDecodedAuthorityRows(branchPages);
  input.onQuery?.({ kind: "thread-authority-streams", rowCount: rows.length });
  const requested = new Set(input.threadIds);
  const nextStreamVersions = new Map(input.previousStreamVersions);
  let previousSequence = input.sequenceExclusive;
  for (const row of rows) {
    const previousStreamVersion = nextStreamVersions.get(row.event.aggregateId);
    if (
      row.event.aggregateKind !== "thread" ||
      !requested.has(row.event.aggregateId) ||
      row.event.sequence <= previousSequence ||
      (row.event.type === "thread.created" && previousStreamVersion !== undefined) ||
      (previousStreamVersion === undefined
        ? row.streamVersion !== 0 && row.streamVersion !== 1
        : row.streamVersion !== previousStreamVersion + 1)
    ) {
      return yield* rawError(`${input.operationPrefix}-thread-group-routing`, "corrupt-history");
    }
    previousSequence = row.event.sequence;
    nextStreamVersions.set(row.event.aggregateId, row.streamVersion);
  }
  return {
    rows,
    nextSequenceExclusive: previousSequence,
    nextStreamVersions,
  };
});

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
    readonly onQuery?: (observation: OrchestrationCommandReplayQueryObservation) => void;
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
      AND command_id IS NOT NULL
      AND CAST(command_id AS BLOB) = ${commandIdBytes}
    ORDER BY sequence
    LIMIT ${RAW_EVENT_PAGE_SIZE}
  `.pipe(
    Effect.mapError((cause) =>
      rawError(`${input.operationPrefix}-read-history`, "persistence", cause),
    ),
  );
  input.onQuery?.({ kind: "command-candidates", rowCount: rawRows.length });
  const rows = yield* decodeRows(rawRows, input.operationPrefix);
  const predecessors = yield* loadImmediatePredecessors(
    sql,
    rows,
    input.operationPrefix,
    input.onQuery,
  );
  let previousSequence = input.sequenceExclusive;
  for (const [index, row] of rows.entries()) {
    if (row.event.commandId !== input.commandId || row.event.sequence <= previousSequence) {
      return yield* rawError(`${input.operationPrefix}-command-routing`, "corrupt-history");
    }
    const predecessor = predecessors[index];
    if (predecessor === null) {
      if (row.streamVersion !== 0 && row.streamVersion !== 1) {
        return yield* rawError(
          `${input.operationPrefix}-stream-predecessor-version`,
          "corrupt-history",
        );
      }
    } else {
      if (
        predecessor === undefined ||
        predecessor.event.aggregateKind !== row.event.aggregateKind ||
        predecessor.event.aggregateId !== row.event.aggregateId ||
        predecessor.event.sequence >= row.event.sequence ||
        row.streamVersion !== predecessor.streamVersion + 1
      ) {
        return yield* rawError(
          `${input.operationPrefix}-stream-predecessor-version`,
          "corrupt-history",
        );
      }
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
