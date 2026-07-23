import {
  AgentControlTaskCreatedPayload,
  AgentControlTaskEvent,
  AgentControlTaskEventDraft,
  AgentControlTaskId,
  AgentControlTaskNeedsAttentionMarkedPayload,
  AgentControlTaskSourceGateChangedPayload,
  CommandId,
  EventId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
  AgentControlTaskStreamVersionConflictError,
} from "../../Errors.ts";
import {
  AgentControlTaskEventStore,
  type AgentControlTaskEventStoreShape,
} from "../Services/AgentControlTaskEventStore.ts";

const DEFAULT_PAGE_SIZE = 500;
const MAX_PAGE_SIZE = 1_000;
const TaskPayload = Schema.Union([
  AgentControlTaskCreatedPayload,
  AgentControlTaskSourceGateChangedPayload,
  AgentControlTaskNeedsAttentionMarkedPayload,
]);
const PersistedRow = Schema.Struct({
  sequence: PositiveInt,
  eventId: EventId,
  type: Schema.Literals([
    "agentControl.task.created",
    "agentControl.task.sourceGate.changed",
    "agentControl.task.needsAttentionMarked",
  ]),
  aggregateKind: Schema.Literal("task"),
  aggregateId: AgentControlTaskId,
  streamVersion: PositiveInt,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: Schema.Literal("controller"),
  payload: Schema.fromJsonString(TaskPayload),
  metadata: Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1) })),
});
const AppendInput = Schema.Struct({
  taskId: AgentControlTaskId,
  expectedStreamVersion: NonNegativeInt,
  events: Schema.Array(AgentControlTaskEventDraft).check(Schema.isNonEmpty()),
});
const decodeAppend = Schema.decodeUnknownEffect(AppendInput);
const decodeRow = Schema.decodeUnknownEffect(PersistedRow);
const decodeEvent = Schema.decodeUnknownEffect(AgentControlTaskEvent);
const decodeInt = Schema.decodeUnknownEffect(NonNegativeInt);
const encodePayload = Schema.encodeUnknownEffect(Schema.fromJsonString(TaskPayload));
const encodeMetadata = Schema.encodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1) })),
);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });
const normalizeLimit = (limit: number | undefined) =>
  Math.max(0, Math.min(MAX_PAGE_SIZE, Math.floor(limit ?? DEFAULT_PAGE_SIZE)));

const makeStore = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const currentVersion = (taskId: AgentControlTaskId) =>
    sql<{ readonly version: unknown }>`
      SELECT COALESCE(MAX(stream_version), 0) AS version
      FROM agent_control_events
      WHERE aggregate_kind = 'task' AND stream_id = ${taskId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlTaskEventStore.currentVersion", cause)),
      Effect.flatMap((rows) =>
        decodeInt(rows[0]?.version).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlTaskEventStore.currentVersion", cause),
          ),
        ),
      ),
    );

  const decodeRows = (
    rows: ReadonlyArray<Record<string, unknown>>,
    operation: string,
  ): Effect.Effect<ReadonlyArray<AgentControlTaskEvent>, AgentControlPersistenceDecodeError> =>
    Effect.forEach(rows, (row) =>
      decodeRow(row).pipe(
        Effect.flatMap(decodeEvent),
        Effect.mapError((cause) => decodeError(operation, cause)),
      ),
    );

  const append: AgentControlTaskEventStoreShape["append"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeAppend(rawInput).pipe(
        Effect.mapError((cause) => decodeError("AgentControlTaskEventStore.append:input", cause)),
      );
      if (input.events.some((event) => event.aggregateId !== input.taskId)) {
        return yield* decodeError(
          "AgentControlTaskEventStore.append:stream-identity",
          new Error("stream identity mismatch"),
        );
      }
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const actualVersion = yield* currentVersion(input.taskId);
            if (actualVersion !== input.expectedStreamVersion) {
              return yield* new AgentControlTaskStreamVersionConflictError({
                taskId: input.taskId,
                expectedVersion: input.expectedStreamVersion,
                actualVersion,
              });
            }
            return yield* Effect.forEach(
              input.events,
              (draft, index) =>
                Effect.gen(function* () {
                  const payload = yield* encodePayload(draft.payload).pipe(
                    Effect.mapError((cause) =>
                      decodeError("AgentControlTaskEventStore.append:payload", cause),
                    ),
                  );
                  const metadata = yield* encodeMetadata(draft.metadata).pipe(
                    Effect.mapError((cause) =>
                      decodeError("AgentControlTaskEventStore.append:metadata", cause),
                    ),
                  );
                  const rows = yield* sql<Record<string, unknown>>`
                    INSERT INTO agent_control_events (
                      event_id, aggregate_kind, stream_id, stream_version, event_type,
                      occurred_at, command_id, causation_event_id, correlation_id,
                      actor_authority, payload_json, metadata_json
                    ) VALUES (
                      ${draft.eventId}, 'task', ${draft.aggregateId},
                      ${input.expectedStreamVersion + index + 1}, ${draft.type},
                      ${draft.occurredAt}, ${draft.commandId}, ${draft.causationEventId},
                      ${draft.correlationId}, 'controller', ${payload}, ${metadata}
                    )
                    RETURNING
                      sequence, event_id AS "eventId", event_type AS "type",
                      aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
                      stream_version AS "streamVersion", occurred_at AS "occurredAt",
                      command_id AS "commandId", causation_event_id AS "causationEventId",
                      correlation_id AS "correlationId", actor_authority AS authority,
                      payload_json AS payload, metadata_json AS metadata
                  `.pipe(
                    Effect.mapError((cause) =>
                      sqlError("AgentControlTaskEventStore.append:insert", cause),
                    ),
                  );
                  const decoded = yield* decodeRows(
                    rows,
                    "AgentControlTaskEventStore.append:decode",
                  );
                  const event = decoded[0];
                  if (event === undefined) {
                    return yield* decodeError(
                      "AgentControlTaskEventStore.append:missing-row",
                      new Error("missing returning row"),
                    );
                  }
                  return event;
                }),
              { concurrency: 1 },
            );
          }),
        )
        .pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(sqlError("AgentControlTaskEventStore.append:transaction", cause)),
          ),
        );
    });

  const readStream: AgentControlTaskEventStoreShape["readStream"] = (taskId, after = 0, limit) => {
    const pageSize = normalizeLimit(limit);
    if (pageSize === 0) return Effect.succeed([]);
    return sql<Record<string, unknown>>`
      SELECT
        sequence, event_id AS "eventId", event_type AS "type",
        aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
        stream_version AS "streamVersion", occurred_at AS "occurredAt",
        command_id AS "commandId", causation_event_id AS "causationEventId",
        correlation_id AS "correlationId", actor_authority AS authority,
        payload_json AS payload, metadata_json AS metadata
      FROM agent_control_events
      WHERE aggregate_kind = 'task' AND stream_id = ${taskId}
        AND stream_version > ${Math.max(0, Math.floor(after))}
      ORDER BY stream_version ASC
      LIMIT ${pageSize}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlTaskEventStore.readStream", cause)),
      Effect.flatMap((rows) => decodeRows(rows, "AgentControlTaskEventStore.readStream")),
    );
  };

  const readGlobal: AgentControlTaskEventStoreShape["readGlobal"] = (after = 0, limit) => {
    const pageSize = normalizeLimit(limit);
    if (pageSize === 0) return Effect.succeed([]);
    return sql<Record<string, unknown>>`
      SELECT
        sequence, event_id AS "eventId", event_type AS "type",
        aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
        stream_version AS "streamVersion", occurred_at AS "occurredAt",
        command_id AS "commandId", causation_event_id AS "causationEventId",
        correlation_id AS "correlationId", actor_authority AS authority,
        payload_json AS payload, metadata_json AS metadata
      FROM agent_control_events
      WHERE aggregate_kind = 'task' AND sequence > ${Math.max(0, Math.floor(after))}
      ORDER BY sequence ASC
      LIMIT ${pageSize}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlTaskEventStore.readGlobal", cause)),
      Effect.flatMap((rows) => decodeRows(rows, "AgentControlTaskEventStore.readGlobal")),
    );
  };

  const latestSequence = sql<{ readonly sequence: unknown }>`
    SELECT COALESCE(MAX(sequence), 0) AS sequence
    FROM agent_control_events
    WHERE aggregate_kind = 'task'
  `.pipe(
    Effect.mapError((cause) => sqlError("AgentControlTaskEventStore.latestSequence", cause)),
    Effect.flatMap((rows) =>
      decodeInt(rows[0]?.sequence).pipe(
        Effect.mapError((cause) => decodeError("AgentControlTaskEventStore.latestSequence", cause)),
      ),
    ),
  );

  return AgentControlTaskEventStore.of({ append, readStream, readGlobal, latestSequence });
});

export const layer = Layer.effect(AgentControlTaskEventStore, makeStore);
