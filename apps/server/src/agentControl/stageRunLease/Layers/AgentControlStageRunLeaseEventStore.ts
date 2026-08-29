import {
  AgentControlStageRunLeaseEvent,
  AgentControlStageRunLeaseEventDraft,
  AgentControlStageRunLeaseId,
  AgentControlStageRunLeaseReleasedAfterImplementationPayload,
  AgentControlStageRunLeaseReleasedAfterPlanningPayload,
  AgentControlStageRunLeaseReleasedAfterVerificationPayload,
  AgentControlStageRunLeaseReleasedPayload,
  AgentControlStageRunLeaseRenewedPayload,
  AgentControlStageRunLeaseReservedPayload,
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
  AgentControlStageRunLeaseStreamVersionConflictError,
} from "../../Errors.ts";
import {
  AgentControlStageRunLeaseEventStore,
  type AgentControlStageRunLeaseEventStoreShape,
} from "../Services/AgentControlStageRunLeaseEventStore.ts";

const DEFAULT_PAGE_SIZE = 500;
const MAX_PAGE_SIZE = 1_000;
const Payload = Schema.Union([
  AgentControlStageRunLeaseReservedPayload,
  AgentControlStageRunLeaseRenewedPayload,
  AgentControlStageRunLeaseReleasedAfterImplementationPayload,
  AgentControlStageRunLeaseReleasedAfterVerificationPayload,
  AgentControlStageRunLeaseReleasedAfterPlanningPayload,
  AgentControlStageRunLeaseReleasedPayload,
]);
const PersistedRow = Schema.Struct({
  sequence: PositiveInt,
  eventId: EventId,
  type: Schema.Literals([
    "agentControl.stageRunLease.reserved",
    "agentControl.stageRunLease.renewed",
    "agentControl.stageRunLease.releasedBeforeExecution",
    "agentControl.stageRunLease.releasedAfterPlanning",
    "agentControl.stageRunLease.releasedAfterImplementation",
    "agentControl.stageRunLease.releasedAfterVerification",
  ]),
  aggregateKind: Schema.Literal("stage-run-lease"),
  aggregateId: AgentControlStageRunLeaseId,
  streamVersion: PositiveInt,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: Schema.Literals(["controller", "system"]),
  payload: Schema.fromJsonString(Payload),
  metadata: Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1) })),
});
const AppendInput = Schema.Struct({
  leaseId: AgentControlStageRunLeaseId,
  expectedStreamVersion: NonNegativeInt,
  events: Schema.Array(AgentControlStageRunLeaseEventDraft).check(Schema.isNonEmpty()),
});
const decodeAppend = Schema.decodeUnknownEffect(AppendInput);
const decodeRow = Schema.decodeUnknownEffect(PersistedRow);
const decodeEvent = Schema.decodeUnknownEffect(AgentControlStageRunLeaseEvent);
const decodeInt = Schema.decodeUnknownEffect(NonNegativeInt);
const encodePayload = Schema.encodeUnknownEffect(Schema.fromJsonString(Payload));
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

  const currentVersion = (leaseId: AgentControlStageRunLeaseId) =>
    sql<{ readonly version: unknown }>`
      SELECT COALESCE(MAX(stream_version), 0) AS version
      FROM agent_control_events
      WHERE aggregate_kind = 'stage-run-lease' AND stream_id = ${leaseId}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlStageRunLeaseEventStore.currentVersion", cause),
      ),
      Effect.flatMap((rows) =>
        decodeInt(rows[0]?.version).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlStageRunLeaseEventStore.currentVersion", cause),
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

  const append: AgentControlStageRunLeaseEventStoreShape["append"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeAppend(rawInput).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlStageRunLeaseEventStore.append:input", cause),
        ),
      );
      if (input.events.some((event) => event.aggregateId !== input.leaseId)) {
        return yield* decodeError(
          "AgentControlStageRunLeaseEventStore.append:stream-identity",
          new Error("stream identity mismatch"),
        );
      }
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const actualVersion = yield* currentVersion(input.leaseId);
          if (actualVersion !== input.expectedStreamVersion) {
            return yield* new AgentControlStageRunLeaseStreamVersionConflictError({
              leaseId: input.leaseId,
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
                    decodeError("AgentControlStageRunLeaseEventStore.append:payload", cause),
                  ),
                );
                const metadata = yield* encodeMetadata(draft.metadata).pipe(
                  Effect.mapError((cause) =>
                    decodeError("AgentControlStageRunLeaseEventStore.append:metadata", cause),
                  ),
                );
                const rows = yield* sql<Record<string, unknown>>`
                  INSERT INTO agent_control_events (
                    event_id, aggregate_kind, stream_id, stream_version, event_type,
                    occurred_at, command_id, causation_event_id, correlation_id,
                    actor_authority, payload_json, metadata_json
                  ) VALUES (
                    ${draft.eventId}, 'stage-run-lease', ${draft.aggregateId},
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
                    sqlError("AgentControlStageRunLeaseEventStore.append:insert", cause),
                  ),
                );
                const decoded = yield* decodeRows(
                  rows,
                  "AgentControlStageRunLeaseEventStore.append:decode",
                );
                const event = decoded[0];
                if (event === undefined) {
                  return yield* decodeError(
                    "AgentControlStageRunLeaseEventStore.append:missing-row",
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
        Effect.fail(sqlError("AgentControlStageRunLeaseEventStore.append:transaction", cause)),
      ),
    );

  const selectRows = (
    leaseId: AgentControlStageRunLeaseId | null,
    after: number,
    limit: number | undefined,
  ) => {
    const pageSize = normalizeLimit(limit);
    if (pageSize === 0) return Effect.succeed([]);
    const query =
      leaseId === null
        ? sql<Record<string, unknown>>`
            SELECT
              sequence, event_id AS "eventId", event_type AS "type",
              aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
              stream_version AS "streamVersion", occurred_at AS "occurredAt",
              command_id AS "commandId", causation_event_id AS "causationEventId",
              correlation_id AS "correlationId", actor_authority AS authority,
              payload_json AS payload, metadata_json AS metadata
            FROM agent_control_events
            WHERE aggregate_kind = 'stage-run-lease'
              AND sequence > ${Math.max(0, Math.floor(after))}
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
            WHERE aggregate_kind = 'stage-run-lease' AND stream_id = ${leaseId}
              AND stream_version > ${Math.max(0, Math.floor(after))}
            ORDER BY stream_version ASC
            LIMIT ${pageSize}
          `;
    return query.pipe(
      Effect.mapError((cause) => sqlError("AgentControlStageRunLeaseEventStore.read", cause)),
      Effect.flatMap((rows) => decodeRows(rows, "AgentControlStageRunLeaseEventStore.read")),
    );
  };

  const readStream: AgentControlStageRunLeaseEventStoreShape["readStream"] = (
    leaseId,
    after = 0,
    limit,
  ) => selectRows(leaseId, after, limit);
  const readGlobal: AgentControlStageRunLeaseEventStoreShape["readGlobal"] = (after = 0, limit) =>
    selectRows(null, after, limit);
  const latestSequence = sql<{ readonly sequence: unknown }>`
    SELECT COALESCE(MAX(sequence), 0) AS sequence
    FROM agent_control_events
    WHERE aggregate_kind = 'stage-run-lease'
  `.pipe(
    Effect.mapError((cause) =>
      sqlError("AgentControlStageRunLeaseEventStore.latestSequence", cause),
    ),
    Effect.flatMap((rows) =>
      decodeInt(rows[0]?.sequence).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlStageRunLeaseEventStore.latestSequence", cause),
        ),
      ),
    ),
  );

  return AgentControlStageRunLeaseEventStore.of({
    append,
    readStream,
    readGlobal,
    latestSequence,
  });
});

export const layer = Layer.effect(AgentControlStageRunLeaseEventStore, make);
