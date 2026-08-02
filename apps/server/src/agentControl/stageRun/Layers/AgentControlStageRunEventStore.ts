import {
  AgentControlStageRunEvent,
  AgentControlStageRunEventDraft,
  AgentControlStageRunId,
  AgentControlStageRunLifecyclePayload,
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
  AgentControlStageRunStreamVersionConflictError,
} from "../../Errors.ts";
import {
  AgentControlStageRunEventStore,
  type AgentControlStageRunEventStoreShape,
} from "../Services/AgentControlStageRunEventStore.ts";

const DEFAULT_PAGE_SIZE = 500;
const MAX_PAGE_SIZE = 1_000;
const PersistedRow = Schema.Struct({
  sequence: PositiveInt,
  eventId: EventId,
  type: Schema.Literals([
    "agentControl.stageRun.prepared",
    "agentControl.stageRun.planningStarted",
    "agentControl.stageRun.planningSucceeded",
    "agentControl.stageRun.planningFailed",
    "agentControl.stageRun.planningCancelled",
  ]),
  aggregateKind: Schema.Literal("stage-run"),
  aggregateId: AgentControlStageRunId,
  streamVersion: PositiveInt,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: Schema.Literals(["controller", "system"]),
  payload: Schema.fromJsonString(AgentControlStageRunLifecyclePayload),
  metadata: Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1) })),
});
const AppendInput = Schema.Struct({
  stageRunId: AgentControlStageRunId,
  expectedStreamVersion: NonNegativeInt,
  events: Schema.Array(AgentControlStageRunEventDraft).check(Schema.isNonEmpty()),
});
const decodeAppend = Schema.decodeUnknownEffect(AppendInput);
const decodeRow = Schema.decodeUnknownEffect(PersistedRow);
const decodeEvent = Schema.decodeUnknownEffect(AgentControlStageRunEvent);
const decodeInt = Schema.decodeUnknownEffect(NonNegativeInt);
const encodePayload = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlStageRunLifecyclePayload),
);
const encodeMetadata = Schema.encodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1) })),
);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });
const normalizeLimit = (limit: number | undefined) =>
  Math.max(0, Math.min(MAX_PAGE_SIZE, Math.floor(limit ?? DEFAULT_PAGE_SIZE)));

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const currentVersion = (stageRunId: AgentControlStageRunId) =>
    sql<{ readonly version: unknown }>`
      SELECT COALESCE(MAX(stream_version), 0) AS version
      FROM agent_control_events
      WHERE aggregate_kind = 'stage-run' AND stream_id = ${stageRunId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlStageRunEventStore.currentVersion", cause)),
      Effect.flatMap((rows) =>
        decodeInt(rows[0]?.version).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlStageRunEventStore.currentVersion", cause),
          ),
        ),
      ),
    );

  const decodeRows = (rows: ReadonlyArray<Record<string, unknown>>, operation: string) =>
    Effect.forEach(rows, (row) =>
      decodeRow(row).pipe(
        Effect.flatMap(decodeEvent),
        Effect.mapError((cause) => decodeError(operation, cause)),
      ),
    );

  const append: AgentControlStageRunEventStoreShape["append"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeAppend(rawInput).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlStageRunEventStore.append:input", cause),
        ),
      );
      if (input.events.some((event) => event.aggregateId !== input.stageRunId)) {
        return yield* decodeError(
          "AgentControlStageRunEventStore.append:stream-identity",
          new Error("stream identity mismatch"),
        );
      }
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const actualVersion = yield* currentVersion(input.stageRunId);
          if (actualVersion !== input.expectedStreamVersion) {
            return yield* new AgentControlStageRunStreamVersionConflictError({
              stageRunId: input.stageRunId,
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
                    decodeError("AgentControlStageRunEventStore.append:payload", cause),
                  ),
                );
                const metadata = yield* encodeMetadata(draft.metadata).pipe(
                  Effect.mapError((cause) =>
                    decodeError("AgentControlStageRunEventStore.append:metadata", cause),
                  ),
                );
                const rows = yield* sql<Record<string, unknown>>`
                  INSERT INTO agent_control_events (
                    event_id, aggregate_kind, stream_id, stream_version, event_type,
                    occurred_at, command_id, causation_event_id, correlation_id,
                    actor_authority, payload_json, metadata_json
                  ) VALUES (
                    ${draft.eventId}, 'stage-run', ${draft.aggregateId},
                    ${input.expectedStreamVersion + index + 1}, ${draft.type},
                    ${draft.occurredAt}, ${draft.commandId}, ${draft.causationEventId},
                    ${draft.correlationId}, ${draft.authority}, ${payload}, ${metadata}
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
                    sqlError("AgentControlStageRunEventStore.append:insert", cause),
                  ),
                );
                const decoded = yield* decodeRows(
                  rows,
                  "AgentControlStageRunEventStore.append:decode",
                );
                const event = decoded[0];
                if (event === undefined) {
                  return yield* decodeError(
                    "AgentControlStageRunEventStore.append:missing-row",
                    new Error("missing returning row"),
                  );
                }
                return event;
              }),
            { concurrency: 1 },
          );
        }),
      );
    }).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(sqlError("AgentControlStageRunEventStore.append:transaction", cause)),
      ),
    );

  const selectRows = (
    stageRunId: AgentControlStageRunId | null,
    after: number,
    limit: number | undefined,
  ) => {
    const pageSize = normalizeLimit(limit);
    if (pageSize === 0) return Effect.succeed([]);
    const query =
      stageRunId === null
        ? sql<Record<string, unknown>>`
            SELECT
              sequence, event_id AS "eventId", event_type AS "type",
              aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
              stream_version AS "streamVersion", occurred_at AS "occurredAt",
              command_id AS "commandId", causation_event_id AS "causationEventId",
              correlation_id AS "correlationId", actor_authority AS authority,
              payload_json AS payload, metadata_json AS metadata
            FROM agent_control_events
            WHERE aggregate_kind = 'stage-run' AND sequence > ${Math.max(0, Math.floor(after))}
            ORDER BY sequence ASC
            LIMIT ${pageSize}
          `
        : sql<Record<string, unknown>>`
            SELECT
              sequence, event_id AS "eventId", event_type AS "type",
              aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
              stream_version AS "streamVersion", occurred_at AS "occurredAt",
              command_id AS "commandId", causation_event_id AS "causationEventId",
              correlation_id AS "correlationId", actor_authority AS authority,
              payload_json AS payload, metadata_json AS metadata
            FROM agent_control_events
            WHERE aggregate_kind = 'stage-run' AND stream_id = ${stageRunId}
              AND stream_version > ${Math.max(0, Math.floor(after))}
            ORDER BY stream_version ASC
            LIMIT ${pageSize}
          `;
    return query.pipe(
      Effect.mapError((cause) => sqlError("AgentControlStageRunEventStore.read", cause)),
      Effect.flatMap((rows) => decodeRows(rows, "AgentControlStageRunEventStore.read")),
    );
  };

  const readStream: AgentControlStageRunEventStoreShape["readStream"] = (
    stageRunId,
    after = 0,
    limit,
  ) => selectRows(stageRunId, after, limit);
  const readGlobal: AgentControlStageRunEventStoreShape["readGlobal"] = (after = 0, limit) =>
    selectRows(null, after, limit);
  const latestSequence = sql<{ readonly sequence: unknown }>`
    SELECT COALESCE(MAX(sequence), 0) AS sequence
    FROM agent_control_events
    WHERE aggregate_kind = 'stage-run'
  `.pipe(
    Effect.mapError((cause) => sqlError("AgentControlStageRunEventStore.latestSequence", cause)),
    Effect.flatMap((rows) =>
      decodeInt(rows[0]?.sequence).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlStageRunEventStore.latestSequence", cause),
        ),
      ),
    ),
  );

  return AgentControlStageRunEventStore.of({
    append,
    readStream,
    readGlobal,
    latestSequence,
  });
});

export const layer = Layer.effect(AgentControlStageRunEventStore, make);
