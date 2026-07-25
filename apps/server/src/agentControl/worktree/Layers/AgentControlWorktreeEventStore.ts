import {
  AgentControlWorktreeEvent,
  AgentControlWorktreeEventDraft,
  AgentControlWorktreeReservationId,
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
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
const EventRow = Schema.Struct({
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
const EnvelopeRow = Schema.Struct({
  eventId: EventId,
  reservationId: AgentControlWorktreeReservationId,
  streamVersion: PositiveInt,
  eventType: EventTypes,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  createdAt: IsoDateTime,
});
const CatalogRow = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  createdAt: IsoDateTime,
  initialEventId: EventId,
  initialStreamVersion: Schema.Literal(1),
});
const AppendInput = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  expectedStreamVersion: NonNegativeInt,
  events: Schema.Array(AgentControlWorktreeEventDraft).check(Schema.isNonEmpty()),
});
const decodeAppend = Schema.decodeUnknownEffect(AppendInput);
const decodeEventRow = Schema.decodeUnknownEffect(EventRow);
const decodeEnvelopeRow = Schema.decodeUnknownEffect(EnvelopeRow);
const decodeCatalogRow = Schema.decodeUnknownEffect(CatalogRow);
const decodeEvent = Schema.decodeUnknownEffect(AgentControlWorktreeEvent);
const decodeReservationId = Schema.decodeUnknownEffect(AgentControlWorktreeReservationId);
const decodeSequence = Schema.decodeUnknownEffect(PositiveInt);
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

  const relationError = (operation: string) =>
    decodeError(operation, new Error("worktree stream catalog/envelope/event bijection mismatch"));

  const readStreamSnapshot: AgentControlWorktreeEventStoreShape["readStreamSnapshot"] = (
    reservationId,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const operation = "AgentControlWorktreeEventStore.readStreamSnapshot";
          const catalogRows = yield* sql<Record<string, unknown>>`
            SELECT reservation_id AS "reservationId", project_id AS "projectId",
              task_id AS "taskId", stage_run_id AS "stageRunId",
              attempt_id AS "attemptId", lease_id AS "leaseId",
              fence_token AS "fenceToken", created_at AS "createdAt",
              initial_event_id AS "initialEventId",
              initial_stream_version AS "initialStreamVersion"
            FROM agent_control_worktree_stream_catalog
            WHERE reservation_id = ${reservationId}
          `;
          const envelopeRows = yield* sql<Record<string, unknown>>`
            SELECT event_id AS "eventId", reservation_id AS "reservationId",
              stream_version AS "streamVersion", event_type AS "eventType",
              project_id AS "projectId", task_id AS "taskId",
              stage_run_id AS "stageRunId", attempt_id AS "attemptId",
              lease_id AS "leaseId", fence_token AS "fenceToken",
              created_at AS "createdAt"
            FROM agent_control_worktree_event_envelopes
            WHERE reservation_id = ${reservationId}
            ORDER BY stream_version ASC, event_id ASC
          `;
          const eventRows = yield* sql<Record<string, unknown>>`
            SELECT sequence, event_id AS "eventId", event_type AS "type",
              aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
              stream_version AS "streamVersion", occurred_at AS "occurredAt",
              command_id AS "commandId", causation_event_id AS "causationEventId",
              correlation_id AS "correlationId", actor_authority AS authority,
              payload_json AS payload, metadata_json AS metadata
            FROM agent_control_events
            WHERE aggregate_kind = 'worktree-reservation'
              AND stream_id = ${reservationId}
            ORDER BY stream_version ASC, event_id ASC
          `;
          const violations = yield* sql<{ readonly count: unknown }>`
            SELECT COUNT(*) AS count
            FROM (
              SELECT 'catalog-without-initial-envelope' AS violation
              FROM agent_control_worktree_stream_catalog AS catalog
              LEFT JOIN agent_control_worktree_event_envelopes AS envelope
                ON envelope.event_id = catalog.initial_event_id
               AND envelope.reservation_id = catalog.reservation_id
               AND envelope.stream_version = catalog.initial_stream_version
              WHERE catalog.reservation_id = ${reservationId}
                AND envelope.event_id IS NULL
              UNION ALL
              SELECT 'initial-envelope-without-catalog'
              FROM agent_control_worktree_event_envelopes AS envelope
              LEFT JOIN agent_control_worktree_stream_catalog AS catalog
                ON catalog.reservation_id = envelope.reservation_id
               AND catalog.initial_event_id = envelope.event_id
               AND catalog.initial_stream_version = envelope.stream_version
              WHERE envelope.reservation_id = ${reservationId}
                AND envelope.stream_version = 1
                AND catalog.reservation_id IS NULL
              UNION ALL
              SELECT 'envelope-without-catalog'
              FROM agent_control_worktree_event_envelopes AS envelope
              LEFT JOIN agent_control_worktree_stream_catalog AS catalog
                ON catalog.reservation_id = envelope.reservation_id
              WHERE envelope.reservation_id = ${reservationId}
                AND catalog.reservation_id IS NULL
              UNION ALL
              SELECT 'envelope-without-event'
              FROM agent_control_worktree_event_envelopes AS envelope
              LEFT JOIN agent_control_events AS event
                ON event.event_id = envelope.event_id
               AND event.aggregate_kind = 'worktree-reservation'
               AND event.stream_id = envelope.reservation_id
               AND event.stream_version = envelope.stream_version
               AND event.event_type = envelope.event_type
              WHERE envelope.reservation_id = ${reservationId}
                AND event.event_id IS NULL
              UNION ALL
              SELECT 'event-without-envelope'
              FROM agent_control_events AS event
              LEFT JOIN agent_control_worktree_event_envelopes AS envelope
                ON envelope.event_id = event.event_id
               AND envelope.reservation_id = event.stream_id
               AND envelope.stream_version = event.stream_version
               AND envelope.event_type = event.event_type
              WHERE event.aggregate_kind = 'worktree-reservation'
                AND event.stream_id = ${reservationId}
                AND envelope.event_id IS NULL
              UNION ALL
              SELECT 'event-envelope-coordinate-mismatch'
              FROM agent_control_events AS event
              JOIN agent_control_worktree_event_envelopes AS envelope
                ON envelope.event_id = event.event_id
              WHERE (event.stream_id = ${reservationId}
                  OR envelope.reservation_id = ${reservationId})
                AND (
                  event.aggregate_kind <> 'worktree-reservation'
                  OR event.stream_id <> envelope.reservation_id
                  OR event.stream_version <> envelope.stream_version
                  OR event.event_type <> envelope.event_type
                )
            )
          `;
          if (
            violations.length !== 1 ||
            typeof violations[0]?.count !== "number" ||
            violations[0].count !== 0
          ) {
            return yield* relationError(operation);
          }
          if (catalogRows.length + envelopeRows.length + eventRows.length === 0) return [];
          if (
            catalogRows.length !== 1 ||
            envelopeRows.length < 1 ||
            eventRows.length < 1 ||
            envelopeRows.length !== eventRows.length
          ) {
            return yield* relationError(operation);
          }
          const catalog = yield* decodeCatalogRow(catalogRows[0]).pipe(
            Effect.mapError((cause) => decodeError(operation, cause)),
          );
          const envelopes = yield* Effect.forEach(envelopeRows, (row) =>
            decodeEnvelopeRow(row).pipe(Effect.mapError((cause) => decodeError(operation, cause))),
          );
          const persistedEvents = yield* Effect.forEach(eventRows, (row) =>
            decodeEventRow(row).pipe(Effect.mapError((cause) => decodeError(operation, cause))),
          );
          const decodedEvents = yield* Effect.forEach(persistedEvents, (row) =>
            decodeEvent(row).pipe(Effect.mapError((cause) => decodeError(operation, cause))),
          );
          const envelopeCoordinates = new Set<string>();
          const eventCoordinates = new Set<string>();
          for (let index = 0; index < decodedEvents.length; index += 1) {
            const event = decodedEvents[index]!;
            const envelope = envelopes[index]!;
            const version = index + 1;
            const envelopeCoordinate = [
              envelope.eventId,
              envelope.reservationId,
              envelope.streamVersion,
              envelope.eventType,
            ].join("\0");
            const eventCoordinate = [
              event.eventId,
              event.aggregateId,
              event.streamVersion,
              event.type,
            ].join("\0");
            envelopeCoordinates.add(envelopeCoordinate);
            eventCoordinates.add(eventCoordinate);
            if (
              envelope.streamVersion !== version ||
              event.streamVersion !== version ||
              envelope.reservationId !== reservationId ||
              event.aggregateId !== reservationId ||
              envelope.projectId !== catalog.projectId ||
              envelope.taskId !== catalog.taskId ||
              envelope.stageRunId !== catalog.stageRunId ||
              envelope.attemptId !== catalog.attemptId ||
              envelope.leaseId !== catalog.leaseId ||
              envelope.fenceToken !== catalog.fenceToken ||
              envelope.createdAt !== event.occurredAt ||
              event.payload.reservationId !== catalog.reservationId ||
              event.payload.projectId !== catalog.projectId ||
              event.payload.taskId !== catalog.taskId ||
              event.payload.stageRunId !== catalog.stageRunId ||
              event.payload.attemptId !== catalog.attemptId ||
              event.payload.leaseId !== catalog.leaseId ||
              event.payload.fenceToken !== catalog.fenceToken
            ) {
              return yield* relationError(operation);
            }
          }
          const initial = decodedEvents[0]!;
          if (
            catalog.reservationId !== reservationId ||
            catalog.initialStreamVersion !== 1 ||
            initial.type !== "agentControl.worktree.reserved" ||
            initial.eventId !== catalog.initialEventId ||
            initial.streamVersion !== catalog.initialStreamVersion ||
            initial.payload.reservedAt !== catalog.createdAt ||
            envelopes[0]?.eventId !== catalog.initialEventId ||
            envelopes[0]?.streamVersion !== catalog.initialStreamVersion ||
            envelopeCoordinates.size !== envelopes.length ||
            eventCoordinates.size !== decodedEvents.length ||
            envelopeCoordinates.size !== eventCoordinates.size ||
            [...envelopeCoordinates].some((coordinate) => !eventCoordinates.has(coordinate))
          ) {
            return yield* relationError(operation);
          }
          return decodedEvents;
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(sqlError("AgentControlWorktreeEventStore.readStreamSnapshot", cause)),
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
                lease_id, fence_token, created_at, initial_event_id, initial_stream_version
              ) VALUES (
                ${input.reservationId}, ${first.payload.projectId}, ${first.payload.taskId},
                ${first.payload.stageRunId}, ${first.payload.attemptId},
                ${first.payload.leaseId}, ${first.payload.fenceToken},
                ${first.payload.reservedAt}, ${first.eventId}, 1
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
                yield* sql`
                  INSERT INTO agent_control_worktree_event_envelopes (
                    event_id, reservation_id, stream_version, event_type,
                    project_id, task_id, stage_run_id, attempt_id, lease_id,
                    fence_token, created_at
                  ) VALUES (
                    ${draft.eventId}, ${draft.aggregateId},
                    ${input.expectedStreamVersion + index + 1}, ${draft.type},
                    ${draft.payload.projectId}, ${draft.payload.taskId},
                    ${draft.payload.stageRunId}, ${draft.payload.attemptId},
                    ${draft.payload.leaseId}, ${draft.payload.fenceToken},
                    ${draft.occurredAt}
                  )
                `.pipe(
                  Effect.mapError((cause) =>
                    sqlError("AgentControlWorktreeEventStore.append:envelopeInsert", cause),
                  ),
                );
                const rows = yield* sql<{ readonly sequence: unknown }>`
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
                  RETURNING sequence
                `.pipe(
                  Effect.mapError((cause) =>
                    sqlError("AgentControlWorktreeEventStore.append:insert", cause),
                  ),
                );
                const sequence = yield* decodeSequence(rows[0]?.sequence).pipe(
                  Effect.mapError((cause) =>
                    decodeError("AgentControlWorktreeEventStore.append:sequence", cause),
                  ),
                );
                if (rows.length !== 1) {
                  return yield* decodeError(
                    "AgentControlWorktreeEventStore.append:missing",
                    new Error("missing appended event"),
                  );
                }
                return yield* decodeEvent({
                  ...draft,
                  streamVersion: input.expectedStreamVersion + index + 1,
                  sequence,
                }).pipe(
                  Effect.mapError((cause) =>
                    decodeError("AgentControlWorktreeEventStore.append:decode", cause),
                  ),
                );
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

  const readStreamIds: AgentControlWorktreeEventStoreShape["readStreamIds"] = sql<{
    readonly reservationId: unknown;
  }>`
      SELECT reservation_id AS "reservationId"
      FROM agent_control_worktree_stream_catalog
      UNION
      SELECT reservation_id AS "reservationId"
      FROM agent_control_worktree_event_envelopes
      UNION
      SELECT stream_id AS "reservationId"
      FROM agent_control_events
      WHERE aggregate_kind = 'worktree-reservation'
      ORDER BY "reservationId" ASC
    `.pipe(
    Effect.mapError((cause) => sqlError("AgentControlWorktreeEventStore.readStreamIds", cause)),
    Effect.flatMap((rows) =>
      Effect.forEach(rows, (row) =>
        decodeReservationId(row.reservationId).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlWorktreeEventStore.readStreamIds", cause),
          ),
        ),
      ),
    ),
  );

  const readGlobal: AgentControlWorktreeEventStoreShape["readGlobal"] = (after = 0, limit) => {
    const pageSize = normalizeLimit(limit);
    if (pageSize === 0) return Effect.succeed([]);
    return sql<Record<string, unknown>>`
      SELECT sequence, event_id AS "eventId", event_type AS "type",
        aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
        stream_version AS "streamVersion", occurred_at AS "occurredAt",
        command_id AS "commandId", causation_event_id AS "causationEventId",
        correlation_id AS "correlationId", actor_authority AS authority,
        payload_json AS payload, metadata_json AS metadata
      FROM agent_control_events
      WHERE aggregate_kind = 'worktree-reservation' AND sequence > ${after}
      ORDER BY sequence ASC LIMIT ${pageSize}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlWorktreeEventStore.readGlobal", cause)),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          decodeEventRow(row).pipe(
            Effect.flatMap((event) => decodeEvent(event)),
            Effect.mapError((cause) =>
              decodeError("AgentControlWorktreeEventStore.readGlobal", cause),
            ),
          ),
        ),
      ),
      Effect.flatMap((page) =>
        Effect.gen(function* () {
          const snapshots = new Map<
            AgentControlWorktreeReservationId,
            ReadonlyArray<AgentControlWorktreeEvent>
          >();
          for (const event of page) {
            if (!snapshots.has(event.aggregateId)) {
              snapshots.set(event.aggregateId, yield* readStreamSnapshot(event.aggregateId));
            }
            if (
              !snapshots
                .get(event.aggregateId)
                ?.some((candidate) => candidate.eventId === event.eventId)
            ) {
              return yield* relationError("AgentControlWorktreeEventStore.readGlobal");
            }
          }
          return page;
        }),
      ),
    );
  };

  return AgentControlWorktreeEventStore.of({
    append,
    readStream: (reservationId, after = 0, limit) => {
      const pageSize = normalizeLimit(limit);
      if (pageSize === 0) return Effect.succeed([]);
      return readStreamSnapshot(reservationId).pipe(
        Effect.map((stream) =>
          stream.filter((event) => event.streamVersion > after).slice(0, pageSize),
        ),
      );
    },
    readStreamSnapshot,
    readStreamIds,
    readGlobal,
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
