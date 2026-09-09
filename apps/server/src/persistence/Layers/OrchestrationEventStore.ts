const READ_PAGE_SIZE = 500;
import {
  CommandId,
  EventId,
  IsoDateTime,
  NonNegativeInt,
  OrchestrationActorKind,
  OrchestrationAggregateKind,
  OrchestrationEvent,
  OrchestrationEventType,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { decodePersistedOrchestrationMetadata } from "../../orchestration/providerRuntimeMessageCorrelation.ts";
import {
  loadOrchestrationEventsAfterSequencePage,
  type OrchestrationEventRawHistoryError,
} from "../../orchestration/orchestrationEventRaw.ts";
import { encodeOrchestrationEventSchemaOrderStorage } from "../../orchestration/orchestrationEventStorage.ts";
import {
  PersistenceDecodeError,
  isPersistenceError,
  toPersistenceDecodeError,
  toPersistenceSqlError,
  type OrchestrationEventStoreError,
} from "../Errors.ts";
import {
  OrchestrationEventStore,
  type OrchestrationEventStoreShape,
} from "../Services/OrchestrationEventStore.ts";

const decodeEvent = Schema.decodeUnknownEffect(OrchestrationEvent);
const UnknownFromJsonString = Schema.fromJsonString(Schema.Unknown);
const decodeUnknownFromJsonString = Schema.decodeUnknownSync(UnknownFromJsonString);
const AppendEventRequestSchema = Schema.Struct({
  eventId: EventId,
  aggregateKind: OrchestrationAggregateKind,
  streamId: Schema.Union([ProjectId, ThreadId]),
  type: OrchestrationEventType,
  causationEventId: Schema.NullOr(EventId),
  correlationId: Schema.NullOr(CommandId),
  actorKind: OrchestrationActorKind,
  occurredAt: IsoDateTime,
  commandId: Schema.NullOr(CommandId),
  payloadJson: Schema.String,
  metadataJson: Schema.String,
});
const AppendMaterializationEventRequestSchema = Schema.Struct({
  ...AppendEventRequestSchema.fields,
  streamVersion: Schema.Literals([1, 2]),
});

const OrchestrationEventPersistedRowSchema = Schema.Struct({
  sequence: NonNegativeInt,
  eventId: EventId,
  type: OrchestrationEventType,
  aggregateKind: OrchestrationAggregateKind,
  aggregateId: Schema.Union([ProjectId, ThreadId]),
  occurredAt: IsoDateTime,
  commandId: Schema.NullOr(CommandId),
  causationEventId: Schema.NullOr(EventId),
  correlationId: Schema.NullOr(CommandId),
  payload: Schema.String,
  metadataText: Schema.String,
  metadataStorageClass: Schema.String,
  metadataBytes: Schema.Unknown,
});

const HasEventAfterRequestSchema = Schema.Struct({
  aggregateKind: Schema.String,
  aggregateId: Schema.String,
  type: Schema.optional(Schema.String),
  sequenceExclusive: NonNegativeInt,
});

const AggregateReplayRequestSchema = Schema.Struct({
  aggregateKind: OrchestrationAggregateKind,
  aggregateId: Schema.String,
  fromSequenceExclusive: NonNegativeInt,
  toSequenceInclusive: NonNegativeInt,
  limit: Schema.Number,
});
const AggregateReplayStatsRowSchema = Schema.Struct({
  eventCount: Schema.Number,
  payloadBytes: Schema.Number,
  hasCreateEvent: Schema.Number,
});
const DEFAULT_READ_FROM_SEQUENCE_LIMIT = 1_000;

const rawHistoryToEventStoreError = (
  cause: OrchestrationEventRawHistoryError,
): OrchestrationEventStoreError =>
  cause.reason === "persistence"
    ? toPersistenceSqlError(cause.operation)(cause)
    : new PersistenceDecodeError({
        operation: cause.operation,
        issue: "invalid-stored-orchestration-event",
        cause,
      });

const decodePersistedEvent = (row: typeof OrchestrationEventPersistedRowSchema.Type) =>
  Effect.try({
    try: () => {
      const { metadataText, metadataStorageClass, metadataBytes, ...event } = row;
      return {
        ...event,
        // Preserve the established payload decoder so unrelated legacy
        // payloads keep their existing compatibility seam.
        payload: decodeUnknownFromJsonString(row.payload),
        metadata: decodePersistedOrchestrationMetadata({
          storageClass: metadataStorageClass,
          bytes: metadataBytes,
          text: metadataText,
        }).value,
      };
    },
    catch: (cause) =>
      new PersistenceDecodeError({
        operation: "OrchestrationEventStore.decodeStoredJson",
        issue: "invalid-stored-json",
        cause,
      }),
  }).pipe(
    Effect.flatMap((event) =>
      decodeEvent(event).pipe(
        Effect.mapError(toPersistenceDecodeError("OrchestrationEventStore.rowToEvent")),
      ),
    ),
  );

const encodePersistedEvent = (
  event: Omit<OrchestrationEvent, "sequence">,
): Effect.Effect<
  ReturnType<typeof encodeOrchestrationEventSchemaOrderStorage>,
  PersistenceDecodeError
> =>
  decodeEvent({ ...event, sequence: 0 }).pipe(
    Effect.map(encodeOrchestrationEventSchemaOrderStorage),
    Effect.mapError(
      (cause) =>
        new PersistenceDecodeError({
          operation: "OrchestrationEventStore.encodeStoredJson",
          issue: "invalid-stored-json",
          cause,
        }),
    ),
  );

function inferActorKind(
  event: Omit<OrchestrationEvent, "sequence">,
): Schema.Schema.Type<typeof OrchestrationActorKind> {
  if (event.commandId !== null && event.commandId.startsWith("provider:")) {
    return "provider";
  }
  if (event.commandId !== null && event.commandId.startsWith("server:")) {
    return "server";
  }
  if (
    event.metadata.providerTurnId !== undefined ||
    event.metadata.providerItemId !== undefined ||
    event.metadata.adapterKey !== undefined
  ) {
    return "provider";
  }
  if (event.commandId === null) {
    return "server";
  }
  return "client";
}

function toPersistenceSqlOrDecodeError(sqlOperation: string, decodeOperation: string) {
  return (cause: unknown): OrchestrationEventStoreError =>
    isPersistenceError(cause)
      ? cause
      : Schema.isSchemaError(cause)
        ? toPersistenceDecodeError(decodeOperation)(cause)
        : toPersistenceSqlError(sqlOperation)(cause);
}

const makeEventStore = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const appendEventRow = SqlSchema.findOne({
    Request: AppendEventRequestSchema,
    Result: OrchestrationEventPersistedRowSchema,
    execute: (request) =>
      sql`
        INSERT INTO main.orchestration_events (
          event_id,
          aggregate_kind,
          stream_id,
          stream_version,
          event_type,
          occurred_at,
          command_id,
          causation_event_id,
          correlation_id,
          actor_kind,
          payload_json,
          metadata_json
        )
        VALUES (
          ${request.eventId},
          ${request.aggregateKind},
          ${request.streamId},
          COALESCE(
            (
              SELECT stream_version + 1
              FROM main.orchestration_events
              WHERE aggregate_kind = ${request.aggregateKind}
                AND stream_id = ${request.streamId}
              ORDER BY stream_version DESC
              LIMIT 1
            ),
            1
          ),
          ${request.type},
          ${request.occurredAt},
          ${request.commandId},
          ${request.causationEventId},
          ${request.correlationId},
          ${request.actorKind},
          ${request.payloadJson},
          ${request.metadataJson}
        )
        RETURNING
          sequence,
          event_id AS "eventId",
          event_type AS "type",
          aggregate_kind AS "aggregateKind",
          stream_id AS "aggregateId",
          occurred_at AS "occurredAt",
          command_id AS "commandId",
          causation_event_id AS "causationEventId",
          correlation_id AS "correlationId",
          payload_json AS "payload",
          metadata_json AS "metadataText",
          typeof(metadata_json) AS "metadataStorageClass",
          CAST(metadata_json AS BLOB) AS "metadataBytes"
      `,
  });

  const appendMaterializationEventRow = SqlSchema.findOne({
    Request: AppendMaterializationEventRequestSchema,
    Result: OrchestrationEventPersistedRowSchema,
    execute: (request) =>
      sql`
        INSERT INTO main.orchestration_events (
          event_id,
          aggregate_kind,
          stream_id,
          stream_version,
          event_type,
          occurred_at,
          command_id,
          causation_event_id,
          correlation_id,
          actor_kind,
          payload_json,
          metadata_json
        )
        VALUES (
          ${request.eventId},
          ${request.aggregateKind},
          ${request.streamId},
          ${request.streamVersion},
          ${request.type},
          ${request.occurredAt},
          ${request.commandId},
          ${request.causationEventId},
          ${request.correlationId},
          ${request.actorKind},
          ${request.payloadJson},
          ${request.metadataJson}
        )
        RETURNING
          sequence,
          event_id AS "eventId",
          event_type AS "type",
          aggregate_kind AS "aggregateKind",
          stream_id AS "aggregateId",
          occurred_at AS "occurredAt",
          command_id AS "commandId",
          causation_event_id AS "causationEventId",
          correlation_id AS "correlationId",
          payload_json AS "payload",
          metadata_json AS "metadataText",
          typeof(metadata_json) AS "metadataStorageClass",
          CAST(metadata_json AS BLOB) AS "metadataBytes"
      `,
  });

  const readAggregateEventRows = SqlSchema.findAll({
    Request: AggregateReplayRequestSchema,
    Result: OrchestrationEventPersistedRowSchema,
    execute: (request) =>
      sql`
        SELECT
          sequence,
          event_id AS "eventId",
          event_type AS "type",
          aggregate_kind AS "aggregateKind",
          stream_id AS "aggregateId",
          occurred_at AS "occurredAt",
          command_id AS "commandId",
          causation_event_id AS "causationEventId",
          correlation_id AS "correlationId",
          payload_json AS "payload",
          metadata_json AS "metadata"
        FROM orchestration_events
        WHERE aggregate_kind = ${request.aggregateKind}
          AND stream_id = ${request.aggregateId}
          AND sequence > ${request.fromSequenceExclusive}
          AND sequence <= ${request.toSequenceInclusive}
        ORDER BY sequence ASC
        LIMIT ${request.limit}
      `,
  });

  const readAggregateReplayStats = SqlSchema.findOne({
    Request: AggregateReplayRequestSchema,
    Result: AggregateReplayStatsRowSchema,
    execute: (request) =>
      sql`
        SELECT
          COUNT(*) AS "eventCount",
          COALESCE(SUM(octet_length(payload_json)), 0) AS "payloadBytes",
          COALESCE(MAX(event_type IN (
            'thread.created', 'project.created'
          )), 0) AS "hasCreateEvent"
        FROM (
          SELECT payload_json, event_type
          FROM orchestration_events
          WHERE aggregate_kind = ${request.aggregateKind}
            AND stream_id = ${request.aggregateId}
            AND sequence > ${request.fromSequenceExclusive}
            AND sequence <= ${request.toSequenceInclusive}
          ORDER BY sequence ASC
          LIMIT ${request.limit}
        )
      `,
  });

  const append: OrchestrationEventStoreShape["append"] = (event) =>
    encodePersistedEvent(event).pipe(
      Effect.flatMap((storage) =>
        appendEventRow({
          eventId: event.eventId,
          aggregateKind: event.aggregateKind,
          streamId: event.aggregateId,
          type: event.type,
          causationEventId: event.causationEventId,
          correlationId: event.correlationId,
          actorKind: inferActorKind(event),
          occurredAt: event.occurredAt,
          commandId: event.commandId,
          payloadJson: storage.payloadJson,
          metadataJson: storage.metadataJson,
        }),
      ),
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "OrchestrationEventStore.append:insert",
          "OrchestrationEventStore.append:decodeRow",
        ),
      ),
      Effect.flatMap(decodePersistedEvent),
    );

  const appendAgentControlThreadMaterialization: OrchestrationEventStoreShape["appendAgentControlThreadMaterialization"] =
    (event, streamVersion) =>
      encodePersistedEvent(event).pipe(
        Effect.flatMap((storage) =>
          appendMaterializationEventRow({
            eventId: event.eventId,
            aggregateKind: event.aggregateKind,
            streamId: event.aggregateId,
            streamVersion,
            type: event.type,
            causationEventId: event.causationEventId,
            correlationId: event.correlationId,
            actorKind: inferActorKind(event),
            occurredAt: event.occurredAt,
            commandId: event.commandId,
            payloadJson: storage.payloadJson,
            metadataJson: storage.metadataJson,
          }),
        ),
        Effect.mapError(
          toPersistenceSqlOrDecodeError(
            "OrchestrationEventStore.appendAgentControlThreadMaterialization:insert",
            "OrchestrationEventStore.appendAgentControlThreadMaterialization:decodeRow",
          ),
        ),
        Effect.flatMap(decodePersistedEvent),
      );

  const readFromSequence: OrchestrationEventStoreShape["readFromSequence"] = (
    sequenceExclusive,
    limit = DEFAULT_READ_FROM_SEQUENCE_LIMIT,
  ) => {
    const normalizedLimit = Math.max(0, Math.floor(limit));
    if (normalizedLimit === 0) {
      return Stream.empty;
    }
    return Stream.paginate(
      { cursor: sequenceExclusive, remaining: normalizedLimit },
      ({ cursor, remaining }) =>
        loadOrchestrationEventsAfterSequencePage(sql, {
          sequenceExclusive: cursor,
          sequenceUpperExclusive: Number.MAX_SAFE_INTEGER + 1,
          limit: remaining,
          operationPrefix: "OrchestrationEventStore.readFromSequence",
        }).pipe(
          Effect.mapError(rawHistoryToEventStoreError),
          Effect.map((page) => {
            const events = page.rows.map((row) => row.event);
            const last = events.at(-1);
            const nextRemaining = remaining - events.length;
            return [
              events,
              last === undefined || nextRemaining <= 0
                ? Option.none()
                : Option.some({ cursor: last.sequence, remaining: nextRemaining }),
            ] as const;
          }),
        ),
    );
  };

  const findEventAfter = SqlSchema.findOneOption({
    Request: HasEventAfterRequestSchema,
    Result: Schema.Struct({ sequence: Schema.Number }),
    execute: (request) => sql`
          SELECT sequence
          FROM orchestration_events
          WHERE aggregate_kind = ${request.aggregateKind}
            AND stream_id = ${request.aggregateId}
            AND ${sql.and([
              sql`sequence > ${request.sequenceExclusive}`,
              ...(request.type === undefined ? [] : [sql`event_type = ${request.type}`]),
            ])}
          LIMIT 1
        `,
  });

  const hasEventAfter: OrchestrationEventStoreShape["hasEventAfter"] = (input) =>
    findEventAfter(input).pipe(
      Effect.map(Option.isSome),
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "OrchestrationEventStore.hasEventAfter:query",
          "OrchestrationEventStore.hasEventAfter:decodeRow",
        ),
      ),
    );

  const readAggregateRange: OrchestrationEventStoreShape["readAggregateRange"] = (input) => {
    const limit = Math.max(0, Math.floor(input.limit ?? DEFAULT_READ_FROM_SEQUENCE_LIMIT));
    if (limit === 0 || input.fromSequenceExclusive >= input.toSequenceInclusive) {
      return Stream.empty;
    }
    return Stream.paginate(
      { cursor: input.fromSequenceExclusive, remaining: limit },
      ({ cursor, remaining }) =>
        readAggregateEventRows({
          ...input,
          fromSequenceExclusive: cursor,
          limit: Math.min(remaining, READ_PAGE_SIZE),
        }).pipe(
          Effect.mapError(
            toPersistenceSqlOrDecodeError(
              "OrchestrationEventStore.readAggregateRange:query",
              "OrchestrationEventStore.readAggregateRange:decodeRows",
            ),
          ),
          Effect.flatMap((rows) =>
            Effect.forEach(rows, (row) =>
              decodeEvent(row).pipe(
                Effect.mapError(
                  toPersistenceDecodeError("OrchestrationEventStore.readAggregateRange:rowToEvent"),
                ),
              ),
            ),
          ),
          Effect.map((events) => {
            const last = events.at(-1);
            const nextRemaining = remaining - events.length;
            return [
              events,
              last === undefined ||
              events.length < READ_PAGE_SIZE ||
              nextRemaining === 0 ||
              last.sequence >= input.toSequenceInclusive
                ? Option.none()
                : Option.some({ cursor: last.sequence, remaining: nextRemaining }),
            ] as const;
          }),
        ),
    );
  };

  const getAggregateReplayStats: OrchestrationEventStoreShape["getAggregateReplayStats"] = (
    input,
  ) =>
    readAggregateReplayStats({
      ...input,
      limit: Math.max(0, Math.floor(input.maxEvents)) + 1,
    }).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "OrchestrationEventStore.getAggregateReplayStats:query",
          "OrchestrationEventStore.getAggregateReplayStats:decodeRow",
        ),
      ),
      Effect.map((row) => ({ ...row, hasCreateEvent: row.hasCreateEvent !== 0 })),
    );

  return {
    append,
    appendAgentControlThreadMaterialization,
    readFromSequence,
    readAggregateRange,
    getAggregateReplayStats,
    readAll: () => readFromSequence(0, Number.MAX_SAFE_INTEGER),
    hasEventAfter,
  } satisfies OrchestrationEventStoreShape;
});

export const OrchestrationEventStoreLive = Layer.effect(OrchestrationEventStore, makeEventStore);
