import {
  AgentControlControlledThreadReservationEvent,
  AgentControlControlledThreadReservationEventDraft,
  AgentControlControlledThreadReservationId,
  AgentControlControlledThreadReservationPreparedPayload,
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
  AgentControlControlledThreadReservationStreamVersionConflictError,
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
} from "../../Errors.ts";
import {
  AgentControlControlledThreadReservationEventStore,
  type AgentControlControlledThreadReservationEventStoreShape,
} from "../Services/AgentControlControlledThreadReservationEventStore.ts";

const DEFAULT_PAGE_SIZE = 500;
const MAX_PAGE_SIZE = 1_000;
const PersistedRow = Schema.Struct({
  sequence: PositiveInt,
  eventId: EventId,
  type: Schema.Literal("agentControl.controlledThreadReservation.prepared"),
  aggregateKind: Schema.Literal("controlled-thread-reservation"),
  aggregateId: AgentControlControlledThreadReservationId,
  streamVersion: Schema.Literal(1),
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: Schema.Literal("controller"),
  payload: Schema.fromJsonString(AgentControlControlledThreadReservationPreparedPayload),
  metadata: Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1) })),
});
const AppendInput = Schema.Struct({
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  expectedStreamVersion: Schema.Literal(0),
  events: Schema.Tuple([AgentControlControlledThreadReservationEventDraft]),
});
const decodeAppend = Schema.decodeUnknownEffect(AppendInput);
const decodeRow = Schema.decodeUnknownEffect(PersistedRow);
const decodeEvent = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationEvent);
const decodeInt = Schema.decodeUnknownEffect(NonNegativeInt);
const encodePayload = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlControlledThreadReservationPreparedPayload),
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

  const currentVersion = (
    controlledThreadReservationId: AgentControlControlledThreadReservationId,
  ) =>
    sql<{ readonly version: unknown }>`
      SELECT COALESCE(MAX(stream_version), 0) AS version
      FROM agent_control_events
      WHERE aggregate_kind = 'controlled-thread-reservation'
        AND stream_id = ${controlledThreadReservationId}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlControlledThreadReservationEventStore.currentVersion", cause),
      ),
      Effect.flatMap((rows) =>
        decodeInt(rows[0]?.version).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlControlledThreadReservationEventStore.currentVersion", cause),
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

  const append: AgentControlControlledThreadReservationEventStoreShape["append"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeAppend(rawInput).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlControlledThreadReservationEventStore.append:input", cause),
        ),
      );
      const draft = input.events[0];
      if (draft.aggregateId !== input.controlledThreadReservationId) {
        return yield* decodeError(
          "AgentControlControlledThreadReservationEventStore.append:identity",
          new Error("stream identity mismatch"),
        );
      }
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const actualVersion = yield* currentVersion(input.controlledThreadReservationId);
          if (actualVersion !== input.expectedStreamVersion) {
            return yield* new AgentControlControlledThreadReservationStreamVersionConflictError({
              controlledThreadReservationId: input.controlledThreadReservationId,
              expectedVersion: input.expectedStreamVersion,
              actualVersion,
            });
          }
          const payload = yield* encodePayload(draft.payload).pipe(
            Effect.mapError((cause) =>
              decodeError(
                "AgentControlControlledThreadReservationEventStore.append:payload",
                cause,
              ),
            ),
          );
          const metadata = yield* encodeMetadata(draft.metadata).pipe(
            Effect.mapError((cause) =>
              decodeError(
                "AgentControlControlledThreadReservationEventStore.append:metadata",
                cause,
              ),
            ),
          );
          yield* sql`
            INSERT INTO agent_control_controlled_thread_stream_catalog (
              controlled_thread_reservation_id, event_id, stream_version,
              command_id, event_type, thread_id, project_id, task_id,
              task_revision, github_intake_sequence, source_identity_fingerprint,
              stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
              attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
              prepared_at
            ) VALUES (
              ${draft.aggregateId}, ${draft.eventId}, 1,
              ${draft.commandId}, ${draft.type}, ${draft.payload.threadId},
              ${draft.payload.projectId}, ${draft.payload.taskId},
              ${draft.payload.taskRevision}, ${draft.payload.githubIntakeSequence},
              ${draft.payload.sourceIdentityFingerprint}, ${draft.payload.stageRunId},
              ${draft.payload.attemptId}, ${draft.payload.roleId},
              ${draft.payload.stageKind}, ${draft.payload.stageOrdinal},
              ${draft.payload.attemptOrdinal}, ${draft.payload.leaseId},
              ${draft.payload.fenceToken}, ${draft.payload.worktreeReservationId},
              ${draft.payload.preparedAt}
            )
          `.pipe(
            Effect.mapError((cause) =>
              sqlError("AgentControlControlledThreadReservationEventStore.append:catalog", cause),
            ),
          );
          const rows = yield* sql<Record<string, unknown>>`
            INSERT INTO agent_control_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type,
              occurred_at, command_id, causation_event_id, correlation_id,
              actor_authority, payload_json, metadata_json
            ) VALUES (
              ${draft.eventId}, 'controlled-thread-reservation', ${draft.aggregateId},
              1, ${draft.type}, ${draft.occurredAt}, ${draft.commandId},
              ${draft.causationEventId}, ${draft.correlationId}, 'controller',
              ${payload}, ${metadata}
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
              sqlError("AgentControlControlledThreadReservationEventStore.append:insert", cause),
            ),
          );
          return yield* decodeRows(
            rows,
            "AgentControlControlledThreadReservationEventStore.append:decode",
          );
        }),
      );
    }).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          sqlError("AgentControlControlledThreadReservationEventStore.append:transaction", cause),
        ),
      ),
    );

  const selectRows = (
    controlledThreadReservationId: AgentControlControlledThreadReservationId | null,
    after: number,
    limit: number | undefined,
  ) => {
    const pageSize = normalizeLimit(limit);
    if (pageSize === 0) return Effect.succeed([]);
    const query =
      controlledThreadReservationId === null
        ? sql<Record<string, unknown>>`
            SELECT
              sequence, event_id AS "eventId", event_type AS "type",
              aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
              stream_version AS "streamVersion", occurred_at AS "occurredAt",
              command_id AS "commandId", causation_event_id AS "causationEventId",
              correlation_id AS "correlationId", actor_authority AS authority,
              payload_json AS payload, metadata_json AS metadata
            FROM agent_control_events
            WHERE aggregate_kind = 'controlled-thread-reservation'
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
            WHERE aggregate_kind = 'controlled-thread-reservation'
              AND stream_id = ${controlledThreadReservationId}
              AND stream_version > ${Math.max(0, Math.floor(after))}
            ORDER BY stream_version ASC
            LIMIT ${pageSize}
          `;
    return query.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlControlledThreadReservationEventStore.read", cause),
      ),
      Effect.flatMap((rows) =>
        decodeRows(rows, "AgentControlControlledThreadReservationEventStore.read"),
      ),
    );
  };

  const readStream: AgentControlControlledThreadReservationEventStoreShape["readStream"] = (
    controlledThreadReservationId,
    after = 0,
    limit,
  ) => selectRows(controlledThreadReservationId, after, limit);
  const readGlobal: AgentControlControlledThreadReservationEventStoreShape["readGlobal"] = (
    after = 0,
    limit,
  ) => selectRows(null, after, limit);
  const latestSequence = sql<{ readonly sequence: unknown }>`
    SELECT COALESCE(MAX(sequence), 0) AS sequence
    FROM agent_control_events
    WHERE aggregate_kind = 'controlled-thread-reservation'
  `.pipe(
    Effect.mapError((cause) =>
      sqlError("AgentControlControlledThreadReservationEventStore.latestSequence", cause),
    ),
    Effect.flatMap((rows) =>
      decodeInt(rows[0]?.sequence).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlControlledThreadReservationEventStore.latestSequence", cause),
        ),
      ),
    ),
  );

  return AgentControlControlledThreadReservationEventStore.of({
    append,
    readStream,
    readGlobal,
    latestSequence,
  });
});

export const layer = Layer.effect(AgentControlControlledThreadReservationEventStore, make);
