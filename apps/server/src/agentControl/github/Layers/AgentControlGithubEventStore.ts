import {
  AgentControlGithubConfigClearedPayload,
  AgentControlGithubConfigSetPayload,
  AgentControlGithubEvent,
  AgentControlGithubEventDraft,
  AgentControlGithubPollFailedPayload,
  AgentControlGithubPollSucceededPayload,
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
} from "../../Errors.ts";
import {
  AgentControlGithubEventStore,
  type AgentControlGithubEventStoreShape,
} from "../Services/AgentControlGithubEventStore.ts";

const DEFAULT_PAGE_SIZE = 500;
const MAX_PAGE_SIZE = 1_000;
const GithubPayload = Schema.Union([
  AgentControlGithubConfigSetPayload,
  AgentControlGithubConfigClearedPayload,
  AgentControlGithubPollSucceededPayload,
  AgentControlGithubPollFailedPayload,
]);

const PersistedRow = Schema.Struct({
  sequence: PositiveInt,
  eventId: EventId,
  type: Schema.Literals([
    "agentControl.github.config.set",
    "agentControl.github.config.cleared",
    "agentControl.github.poll.succeeded",
    "agentControl.github.poll.failed",
  ]),
  aggregateKind: Schema.Literal("github-intake"),
  aggregateId: ProjectId,
  streamVersion: PositiveInt,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: Schema.Literals(["human", "controller", "system"]),
  payload: Schema.fromJsonString(GithubPayload),
  metadata: Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1) })),
});

const AppendInput = Schema.Struct({
  projectId: ProjectId,
  expectedStreamVersion: NonNegativeInt,
  events: Schema.Array(AgentControlGithubEventDraft).check(Schema.isNonEmpty()),
});

const decodeAppend = Schema.decodeUnknownEffect(AppendInput);
const decodeRow = Schema.decodeUnknownEffect(PersistedRow);
const decodeEvent = Schema.decodeUnknownEffect(AgentControlGithubEvent);
const decodeInt = Schema.decodeUnknownEffect(NonNegativeInt);
const encodePayload = Schema.encodeUnknownEffect(Schema.fromJsonString(GithubPayload));
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

  const currentVersion = (projectId: ProjectId) =>
    sql<{ readonly version: unknown }>`
      SELECT COALESCE(MAX(stream_version), 0) AS version
      FROM agent_control_events
      WHERE aggregate_kind = 'github-intake' AND stream_id = ${projectId}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlGithubEventStore.currentVersion", cause)),
      Effect.flatMap((rows) =>
        decodeInt(rows[0]?.version).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlGithubEventStore.currentVersion", cause),
          ),
        ),
      ),
    );

  const decodeRows = (
    rows: ReadonlyArray<Record<string, unknown>>,
    operation: string,
  ): Effect.Effect<ReadonlyArray<AgentControlGithubEvent>, AgentControlPersistenceDecodeError> =>
    Effect.forEach(rows, (row) =>
      decodeRow(row).pipe(
        Effect.flatMap(decodeEvent),
        Effect.mapError((cause) => decodeError(operation, cause)),
      ),
    );

  const append: AgentControlGithubEventStoreShape["append"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeAppend(rawInput).pipe(
        Effect.mapError((cause) => decodeError("AgentControlGithubEventStore.append:input", cause)),
      );
      if (input.events.some((event) => event.aggregateId !== input.projectId)) {
        return yield* decodeError(
          "AgentControlGithubEventStore.append:stream-identity",
          new Error("stream identity mismatch"),
        );
      }
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const actualVersion = yield* currentVersion(input.projectId);
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
                  const payload = yield* encodePayload(draft.payload).pipe(
                    Effect.mapError((cause) =>
                      decodeError("AgentControlGithubEventStore.append:payload", cause),
                    ),
                  );
                  const metadata = yield* encodeMetadata(draft.metadata).pipe(
                    Effect.mapError((cause) =>
                      decodeError("AgentControlGithubEventStore.append:metadata", cause),
                    ),
                  );
                  const rows = yield* sql<Record<string, unknown>>`
                  INSERT INTO agent_control_events (
                    event_id, aggregate_kind, stream_id, stream_version, event_type,
                    occurred_at, command_id, causation_event_id, correlation_id,
                    actor_authority, payload_json, metadata_json
                  ) VALUES (
                    ${draft.eventId}, ${draft.aggregateKind}, ${draft.aggregateId},
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
                      sqlError("AgentControlGithubEventStore.append:insert", cause),
                    ),
                  );
                  const decoded = yield* decodeRows(
                    rows,
                    "AgentControlGithubEventStore.append:decode",
                  );
                  const event = decoded[0];
                  if (event === undefined) {
                    return yield* decodeError(
                      "AgentControlGithubEventStore.append:missing-row",
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
            Effect.fail(sqlError("AgentControlGithubEventStore.append:transaction", cause)),
          ),
        );
    });

  const readStream: AgentControlGithubEventStoreShape["readStream"] = (
    projectId,
    after = 0,
    limit,
  ) => {
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
      WHERE aggregate_kind = 'github-intake'
        AND stream_id = ${projectId}
        AND stream_version > ${Math.max(0, Math.floor(after))}
      ORDER BY stream_version ASC
      LIMIT ${pageSize}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlGithubEventStore.readStream", cause)),
      Effect.flatMap((rows) => decodeRows(rows, "AgentControlGithubEventStore.readStream")),
    );
  };

  const readGlobal: AgentControlGithubEventStoreShape["readGlobal"] = (after = 0, limit) => {
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
      WHERE aggregate_kind = 'github-intake'
        AND sequence > ${Math.max(0, Math.floor(after))}
      ORDER BY sequence ASC
      LIMIT ${pageSize}
    `.pipe(
      Effect.mapError((cause) => sqlError("AgentControlGithubEventStore.readGlobal", cause)),
      Effect.flatMap((rows) => decodeRows(rows, "AgentControlGithubEventStore.readGlobal")),
    );
  };

  const readProjectAfterSequence: AgentControlGithubEventStoreShape["readProjectAfterSequence"] = (
    projectId,
    after = 0,
    limit,
  ) => {
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
      WHERE aggregate_kind = 'github-intake'
        AND stream_id = ${projectId}
        AND sequence > ${Math.max(0, Math.floor(after))}
      ORDER BY sequence ASC
      LIMIT ${pageSize}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlGithubEventStore.readProjectAfterSequence", cause),
      ),
      Effect.flatMap((rows) =>
        decodeRows(rows, "AgentControlGithubEventStore.readProjectAfterSequence"),
      ),
    );
  };

  const latestSequence = sql<{ readonly sequence: unknown }>`
    SELECT COALESCE(MAX(sequence), 0) AS sequence
    FROM agent_control_events
    WHERE aggregate_kind = 'github-intake'
  `.pipe(
    Effect.mapError((cause) => sqlError("AgentControlGithubEventStore.latestSequence", cause)),
    Effect.flatMap((rows) =>
      decodeInt(rows[0]?.sequence).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlGithubEventStore.latestSequence", cause),
        ),
      ),
    ),
  );

  return AgentControlGithubEventStore.of({
    append,
    readStream,
    readGlobal,
    readProjectAfterSequence,
    latestSequence,
  });
});

export const layer = Layer.effect(AgentControlGithubEventStore, makeStore);
