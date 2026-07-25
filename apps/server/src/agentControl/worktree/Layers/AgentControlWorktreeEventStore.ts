import {
  AgentControlWorktreeEvent,
  AgentControlWorktreeEventDraft,
  AgentControlWorktreeReservationId,
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
  AgentControlWorktreeStreamVersionConflictError,
} from "../../Errors.ts";
import {
  AgentControlWorktreeEventStore,
  type AgentControlWorktreeEventStoreShape,
} from "../Services/AgentControlWorktreeEventStore.ts";

const DEFAULT_PAGE_SIZE = 500;
const MAX_PAGE_SIZE = 1_000;
const EventTypes = Schema.Literals([
  "agentControl.worktree.reserved",
  "agentControl.worktree.materializationStarted",
  "agentControl.worktree.ready",
  "agentControl.worktree.needsAttention",
]);
const PersistedRow = Schema.Struct({
  sequence: PositiveInt,
  eventId: EventId,
  type: EventTypes,
  aggregateKind: Schema.Literal("worktree-reservation"),
  aggregateId: AgentControlWorktreeReservationId,
  streamVersion: PositiveInt,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: Schema.Literal("controller"),
  payload: Schema.fromJsonString(Schema.Unknown),
  metadata: Schema.fromJsonString(Schema.Unknown),
});
const AppendInput = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  expectedStreamVersion: NonNegativeInt,
  events: Schema.Array(AgentControlWorktreeEventDraft).check(Schema.isNonEmpty()),
});
const decodeAppend = Schema.decodeUnknownEffect(AppendInput);
const decodeRow = Schema.decodeUnknownEffect(PersistedRow);
const decodeEvent = Schema.decodeUnknownEffect(AgentControlWorktreeEvent);
const decodeInt = Schema.decodeUnknownEffect(NonNegativeInt);
const encodeJson = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });
const normalizeLimit = (limit: number | undefined) =>
  Math.max(0, Math.min(MAX_PAGE_SIZE, Math.floor(limit ?? DEFAULT_PAGE_SIZE)));

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const currentVersion = (reservationId: AgentControlWorktreeReservationId) =>
    sql<{ readonly version: unknown }>`
      SELECT COALESCE(MAX(stream_version), 0) AS version
      FROM agent_control_events
      WHERE aggregate_kind = 'worktree-reservation' AND stream_id = ${reservationId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlWorktreeEventStore.currentVersion", cause)),
      Effect.flatMap((rows) =>
        decodeInt(rows[0]?.version).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlWorktreeEventStore.currentVersion", cause),
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

  const append: AgentControlWorktreeEventStoreShape["append"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeAppend(rawInput).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlWorktreeEventStore.append:input", cause),
        ),
      );
      if (input.events.some((event) => event.aggregateId !== input.reservationId)) {
        return yield* decodeError(
          "AgentControlWorktreeEventStore.append:identity",
          new Error("stream identity mismatch"),
        );
      }
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const actualVersion = yield* currentVersion(input.reservationId);
          if (actualVersion !== input.expectedStreamVersion) {
            return yield* new AgentControlWorktreeStreamVersionConflictError({
              reservationId: input.reservationId,
              expectedVersion: input.expectedStreamVersion,
              actualVersion,
            });
          }
          const first = input.events[0]!;
          if (input.expectedStreamVersion === 0) {
            if (
              first.type !== "agentControl.worktree.reserved" ||
              first.aggregateId !== input.reservationId ||
              first.payload.reservationId !== input.reservationId
            ) {
              return yield* decodeError(
                "AgentControlWorktreeEventStore.append:catalogIdentity",
                new Error("initial event does not define the reservation catalog identity"),
              );
            }
            const catalog = yield* sql<{ readonly reservationId: unknown }>`
              INSERT INTO agent_control_worktree_stream_catalog (
                reservation_id, project_id, task_id, stage_run_id, attempt_id,
                lease_id, fence_token, created_at
              ) VALUES (
                ${input.reservationId}, ${first.payload.projectId}, ${first.payload.taskId},
                ${first.payload.stageRunId}, ${first.payload.attemptId},
                ${first.payload.leaseId}, ${first.payload.fenceToken},
                ${first.payload.reservedAt}
              )
              ON CONFLICT(reservation_id) DO NOTHING
              RETURNING reservation_id AS "reservationId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlWorktreeEventStore.append:catalogInsert", cause),
              ),
            );
            if (catalog.length !== 1 || catalog[0]?.reservationId !== input.reservationId) {
              return yield* decodeError(
                "AgentControlWorktreeEventStore.append:catalogConflict",
                new Error("reservation catalog identity already exists"),
              );
            }
          }
          const catalog = yield* sql<{
            readonly reservationId: unknown;
            readonly projectId: unknown;
            readonly taskId: unknown;
            readonly stageRunId: unknown;
            readonly attemptId: unknown;
            readonly leaseId: unknown;
            readonly fenceToken: unknown;
          }>`
            SELECT reservation_id AS "reservationId", project_id AS "projectId",
              task_id AS "taskId", stage_run_id AS "stageRunId",
              attempt_id AS "attemptId", lease_id AS "leaseId",
              fence_token AS "fenceToken"
            FROM agent_control_worktree_stream_catalog
            WHERE reservation_id = ${input.reservationId}
          `.pipe(
            Effect.mapError((cause) =>
              sqlError("AgentControlWorktreeEventStore.append:catalogRead", cause),
            ),
          );
          const identity = catalog[0];
          if (
            catalog.length !== 1 ||
            identity?.reservationId !== input.reservationId ||
            input.events.some(
              (event) =>
                event.payload.reservationId !== input.reservationId ||
                event.payload.projectId !== identity.projectId ||
                event.payload.taskId !== identity.taskId ||
                event.payload.stageRunId !== identity.stageRunId ||
                event.payload.attemptId !== identity.attemptId ||
                event.payload.leaseId !== identity.leaseId ||
                event.payload.fenceToken !== identity.fenceToken,
            )
          ) {
            return yield* decodeError(
              "AgentControlWorktreeEventStore.append:catalogIdentity",
              new Error("event identity does not match the immutable reservation catalog"),
            );
          }
          return yield* Effect.forEach(
            input.events,
            (draft, index) =>
              Effect.gen(function* () {
                const payload = yield* encodeJson(draft.payload).pipe(
                  Effect.mapError((cause) =>
                    decodeError("AgentControlWorktreeEventStore.append:payload", cause),
                  ),
                );
                const metadata = yield* encodeJson(draft.metadata).pipe(
                  Effect.mapError((cause) =>
                    decodeError("AgentControlWorktreeEventStore.append:metadata", cause),
                  ),
                );
                const rows = yield* sql<Record<string, unknown>>`
                  INSERT INTO agent_control_events (
                    event_id, aggregate_kind, stream_id, stream_version, event_type,
                    occurred_at, command_id, causation_event_id, correlation_id,
                    actor_authority, payload_json, metadata_json
                  ) VALUES (
                    ${draft.eventId}, 'worktree-reservation', ${draft.aggregateId},
                    ${input.expectedStreamVersion + index + 1}, ${draft.type},
                    ${draft.occurredAt}, ${draft.commandId}, ${draft.causationEventId},
                    ${draft.correlationId}, 'controller', ${payload}, ${metadata}
                  )
                  RETURNING sequence, event_id AS "eventId", event_type AS "type",
                    aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
                    stream_version AS "streamVersion", occurred_at AS "occurredAt",
                    command_id AS "commandId", causation_event_id AS "causationEventId",
                    correlation_id AS "correlationId", actor_authority AS authority,
                    payload_json AS payload, metadata_json AS metadata
                `.pipe(
                  Effect.mapError((cause) =>
                    sqlError("AgentControlWorktreeEventStore.append:insert", cause),
                  ),
                );
                const decoded = yield* decodeRows(
                  rows,
                  "AgentControlWorktreeEventStore.append:decode",
                );
                const event = decoded[0];
                if (event === undefined) {
                  return yield* decodeError(
                    "AgentControlWorktreeEventStore.append:missing",
                    new Error("missing appended event"),
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
        Effect.fail(sqlError("AgentControlWorktreeEventStore.append:transaction", cause)),
      ),
    );

  const read = (
    reservationId: AgentControlWorktreeReservationId | null,
    after: number,
    limit: number | undefined,
  ) => {
    const pageSize = normalizeLimit(limit);
    if (pageSize === 0) return Effect.succeed([]);
    const query =
      reservationId === null
        ? sql<Record<string, unknown>>`
            SELECT sequence, event_id AS "eventId", event_type AS "type",
              aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
              stream_version AS "streamVersion", occurred_at AS "occurredAt",
              command_id AS "commandId", causation_event_id AS "causationEventId",
              correlation_id AS "correlationId", actor_authority AS authority,
              payload_json AS payload, metadata_json AS metadata
            FROM agent_control_events
            WHERE aggregate_kind = 'worktree-reservation' AND sequence > ${after}
            ORDER BY sequence ASC LIMIT ${pageSize}
          `
        : sql<Record<string, unknown>>`
            SELECT sequence, event_id AS "eventId", event_type AS "type",
              aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
              stream_version AS "streamVersion", occurred_at AS "occurredAt",
              command_id AS "commandId", causation_event_id AS "causationEventId",
              correlation_id AS "correlationId", actor_authority AS authority,
              payload_json AS payload, metadata_json AS metadata
            FROM agent_control_events
            WHERE aggregate_kind = 'worktree-reservation' AND stream_id = ${reservationId}
              AND stream_version > ${after}
            ORDER BY stream_version ASC LIMIT ${pageSize}
          `;
    return query.pipe(
      Effect.mapError((cause) => sqlError("AgentControlWorktreeEventStore.read", cause)),
      Effect.flatMap((rows) => decodeRows(rows, "AgentControlWorktreeEventStore.read:decode")),
    );
  };

  return AgentControlWorktreeEventStore.of({
    append,
    readStream: (reservationId, after = 0, limit) => read(reservationId, after, limit),
    readGlobal: (after = 0, limit) => read(null, after, limit),
    latestSequence: sql<{ readonly sequence: unknown }>`
      SELECT COALESCE(MAX(sequence), 0) AS sequence
      FROM agent_control_events WHERE aggregate_kind = 'worktree-reservation'
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlWorktreeEventStore.latestSequence", cause)),
      Effect.flatMap((rows) =>
        decodeInt(rows[0]?.sequence).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlWorktreeEventStore.latestSequence", cause),
          ),
        ),
      ),
    ),
  });
});

export const layer = Layer.effect(AgentControlWorktreeEventStore, make);
