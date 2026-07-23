import {
  AgentControlEvent,
  AgentControlEventMetadata,
  AgentControlEventAuthority,
  AgentControlProjectModeChangedPayload,
  AgentControlProjectModeChangedEventDraft,
  CommandId,
  EventId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
  AgentControlStreamVersionConflictError,
} from "../../agentControl/Errors.ts";
import {
  AgentControlEventStore,
  type AgentControlEventStoreShape,
} from "../Services/AgentControlEventStore.ts";

const DEFAULT_PAGE_SIZE = 500;
const MAX_PAGE_SIZE = 1_000;

const PersistedEventRow = Schema.Struct({
  sequence: PositiveInt,
  eventId: EventId,
  type: Schema.Literal("agentControl.project.mode.changed"),
  aggregateKind: Schema.Literal("project-controller"),
  aggregateId: ProjectId,
  streamVersion: PositiveInt,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: AgentControlEventAuthority,
  payload: Schema.fromJsonString(AgentControlProjectModeChangedPayload),
  metadata: Schema.fromJsonString(AgentControlEventMetadata),
});

const AppendInput = Schema.Struct({
  projectId: ProjectId,
  expectedStreamVersion: NonNegativeInt,
  events: Schema.Array(AgentControlProjectModeChangedEventDraft).check(Schema.isNonEmpty()),
});

const decodeAppendInput = Schema.decodeUnknownEffect(AppendInput);
const decodePersistedRow = Schema.decodeUnknownEffect(PersistedEventRow);
const decodeEvent = Schema.decodeUnknownEffect(AgentControlEvent);
const decodeNonNegativeInt = Schema.decodeUnknownEffect(NonNegativeInt);
const encodePayloadJson = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlProjectModeChangedPayload),
);
const encodeMetadataJson = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlEventMetadata),
);

const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });

const normalizeLimit = (limit: number | undefined) =>
  Math.max(0, Math.min(MAX_PAGE_SIZE, Math.floor(limit ?? DEFAULT_PAGE_SIZE)));

const makeAgentControlEventStore = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const currentStreamVersion = (projectId: ProjectId) =>
    sql<{ readonly version: unknown }>`
      SELECT COALESCE(MAX(stream_version), 0) AS version
      FROM agent_control_events
      WHERE aggregate_kind = 'project-controller'
        AND stream_id = ${projectId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlEventStore.currentStreamVersion", cause)),
      Effect.flatMap((rows) =>
        decodeNonNegativeInt(rows[0]?.version).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlEventStore.currentStreamVersion", cause),
          ),
        ),
      ),
    );

  const decodeRows = (
    rows: ReadonlyArray<Record<string, unknown>>,
    operation: string,
  ): Effect.Effect<ReadonlyArray<AgentControlEvent>, AgentControlPersistenceDecodeError> =>
    Effect.forEach(rows, (row) =>
      decodePersistedRow(row).pipe(
        Effect.flatMap((decoded) => decodeEvent(decoded)),
        Effect.mapError((cause) => decodeError(operation, cause)),
      ),
    );

  const append: AgentControlEventStoreShape["append"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeAppendInput(rawInput).pipe(
        Effect.mapError((cause) => decodeError("AgentControlEventStore.append:input", cause)),
      );
      if (input.events.some((event) => event.aggregateId !== input.projectId)) {
        return yield* decodeError(
          "AgentControlEventStore.append:stream-identity",
          new Error("event stream identity mismatch"),
        );
      }

      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const actualVersion = yield* currentStreamVersion(input.projectId);
            if (actualVersion !== input.expectedStreamVersion) {
              return yield* new AgentControlStreamVersionConflictError({
                projectId: input.projectId,
                expectedVersion: input.expectedStreamVersion,
                actualVersion,
              });
            }

            return yield* Effect.forEach(
              input.events,
              (draft, index) =>
                Effect.gen(function* () {
                  const streamVersion = input.expectedStreamVersion + index + 1;
                  const payloadJson = yield* encodePayloadJson(draft.payload).pipe(
                    Effect.mapError((cause) =>
                      decodeError("AgentControlEventStore.append:encode-payload", cause),
                    ),
                  );
                  const metadataJson = yield* encodeMetadataJson(draft.metadata).pipe(
                    Effect.mapError((cause) =>
                      decodeError("AgentControlEventStore.append:encode-metadata", cause),
                    ),
                  );
                  const rows = yield* sql<Record<string, unknown>>`
                    INSERT INTO agent_control_events (
                      event_id,
                      aggregate_kind,
                      stream_id,
                      stream_version,
                      event_type,
                      occurred_at,
                      command_id,
                      causation_event_id,
                      correlation_id,
                      actor_authority,
                      payload_json,
                      metadata_json
                    ) VALUES (
                      ${draft.eventId},
                      ${draft.aggregateKind},
                      ${draft.aggregateId},
                      ${streamVersion},
                      ${draft.type},
                      ${draft.occurredAt},
                      ${draft.commandId},
                      ${draft.causationEventId},
                      ${draft.correlationId},
                      ${draft.authority},
                      ${payloadJson},
                      ${metadataJson}
                    )
                    RETURNING
                      sequence,
                      event_id AS "eventId",
                      event_type AS "type",
                      aggregate_kind AS "aggregateKind",
                      stream_id AS "aggregateId",
                      stream_version AS "streamVersion",
                      occurred_at AS "occurredAt",
                      command_id AS "commandId",
                      causation_event_id AS "causationEventId",
                      correlation_id AS "correlationId",
                      actor_authority AS authority,
                      payload_json AS payload,
                      metadata_json AS metadata
                  `.pipe(
                    Effect.mapError((cause) =>
                      sqlError("AgentControlEventStore.append:insert", cause),
                    ),
                  );
                  const decoded = yield* decodeRows(rows, "AgentControlEventStore.append:decode");
                  const event = decoded[0];
                  if (event === undefined) {
                    return yield* decodeError(
                      "AgentControlEventStore.append:missing-returning-row",
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
            Effect.fail(sqlError("AgentControlEventStore.append:transaction", cause)),
          ),
          Effect.catchTag("AgentControlPersistenceSqlError", (error) =>
            Effect.gen(function* () {
              const actualVersion = yield* currentStreamVersion(input.projectId);
              if (actualVersion !== input.expectedStreamVersion) {
                return yield* new AgentControlStreamVersionConflictError({
                  projectId: input.projectId,
                  expectedVersion: input.expectedStreamVersion,
                  actualVersion,
                });
              }
              return yield* error;
            }),
          ),
        );
    });

  const readStream: AgentControlEventStoreShape["readStream"] = (
    projectId,
    afterStreamVersion = 0,
    limit,
  ) => {
    const pageSize = normalizeLimit(limit);
    if (pageSize === 0) return Effect.succeed([]);
    return sql<Record<string, unknown>>`
      SELECT
        sequence,
        event_id AS "eventId",
        event_type AS "type",
        aggregate_kind AS "aggregateKind",
        stream_id AS "aggregateId",
        stream_version AS "streamVersion",
        occurred_at AS "occurredAt",
        command_id AS "commandId",
        causation_event_id AS "causationEventId",
        correlation_id AS "correlationId",
        actor_authority AS authority,
        payload_json AS payload,
        metadata_json AS metadata
      FROM agent_control_events
      WHERE aggregate_kind = 'project-controller'
        AND stream_id = ${projectId}
        AND stream_version > ${Math.max(0, Math.floor(afterStreamVersion))}
      ORDER BY stream_version ASC
      LIMIT ${pageSize}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlEventStore.readStream:query", cause)),
      Effect.flatMap((rows) => decodeRows(rows, "AgentControlEventStore.readStream:decode")),
    );
  };

  const readGlobal: AgentControlEventStoreShape["readGlobal"] = (afterSequence = 0, limit) => {
    const pageSize = normalizeLimit(limit);
    if (pageSize === 0) return Effect.succeed([]);
    return sql<Record<string, unknown>>`
      SELECT
        sequence,
        event_id AS "eventId",
        event_type AS "type",
        aggregate_kind AS "aggregateKind",
        stream_id AS "aggregateId",
        stream_version AS "streamVersion",
        occurred_at AS "occurredAt",
        command_id AS "commandId",
        causation_event_id AS "causationEventId",
        correlation_id AS "correlationId",
        actor_authority AS authority,
        payload_json AS payload,
        metadata_json AS metadata
      FROM agent_control_events
      WHERE aggregate_kind = 'project-controller'
        AND sequence > ${Math.max(0, Math.floor(afterSequence))}
      ORDER BY sequence ASC
      LIMIT ${pageSize}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlEventStore.readGlobal:query", cause)),
      Effect.flatMap((rows) => decodeRows(rows, "AgentControlEventStore.readGlobal:decode")),
    );
  };

  const latestSequence = sql<{ readonly sequence: unknown }>`
    SELECT COALESCE(MAX(sequence), 0) AS sequence
    FROM agent_control_events
    WHERE aggregate_kind = 'project-controller'
  `.pipe(
    Effect.mapError((cause) => sqlError("AgentControlEventStore.latestSequence:query", cause)),
    Effect.flatMap((rows) =>
      decodeNonNegativeInt(rows[0]?.sequence).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlEventStore.latestSequence:decode", cause),
        ),
      ),
    ),
  );

  return AgentControlEventStore.of({ append, readStream, readGlobal, latestSequence });
});

export const AgentControlEventStoreLive = Layer.effect(
  AgentControlEventStore,
  makeAgentControlEventStore,
);
