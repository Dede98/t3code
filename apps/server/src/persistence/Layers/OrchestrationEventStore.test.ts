import * as Schema from "effect/Schema";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RuntimeItemId,
  ThreadId,
  TurnId,
  type OrchestrationEvent,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ServerConfig } from "../../config.ts";
import { OrchestrationProjectionPipelineLive } from "../../orchestration/Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionPipeline } from "../../orchestration/Services/ProjectionPipeline.ts";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import { OrchestrationEventStore } from "../Services/OrchestrationEventStore.ts";
import { OrchestrationEventStoreLive } from "./OrchestrationEventStore.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";
import * as NodeV8 from "node:v8";
import { PersistenceDecodeError } from "../Errors.ts";

function messageEvent(threadId: ThreadId, id: string): Omit<OrchestrationEvent, "sequence"> {
  const now = "2026-01-01T00:00:00.000Z";
  return {
    type: "thread.message-sent",
    eventId: EventId.make(id),
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    payload: {
      threadId,
      messageId: MessageId.make(id),
      role: "assistant",
      text: id,
      turnId: null,
      streaming: false,
      createdAt: now,
      updatedAt: now,
    },
  };
}

const layer = it.layer(
  OrchestrationEventStoreLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);

layer("OrchestrationEventStore", (it) => {
  it.effect("rejects malformed JSON at the orchestration storage boundary", () =>
    Effect.gen(function* () {
      const eventStore = yield* OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-01-01T00:00:00.000Z";

      const insertResult = yield* Effect.result(sql`
        INSERT INTO orchestration_events (
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
          ${EventId.make("evt-store-invalid-json")},
          ${"project"},
          ${ProjectId.make("project-invalid-json")},
          ${0},
          ${"project.created"},
          ${now},
          ${CommandId.make("cmd-store-invalid-json")},
          ${null},
          ${null},
          ${"server"},
          ${"{"},
          ${"{}"}
        )
      `);

      assert.equal(insertResult._tag, "Failure");
      assert.deepStrictEqual(
        yield* sql`SELECT event_id FROM main.orchestration_events
          WHERE event_id='evt-store-invalid-json'`,
        [],
      );
      const replayed = Array.from(yield* Stream.runCollect(eventStore.readFromSequence(0, 10)));
      assert.lengthOf(replayed, 1);
      assert.equal(replayed[0]?.eventId, "evt-store-roundtrip");
    }),
  );

  it.effect("writes the schema-order payload and five-field correlation storage family", () =>
    Effect.gen(function* () {
      const eventStore = yield* OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-08-28T10:00:00.000Z";
      const runtimeEventId = EventId.make("event-current-store");
      const threadId = ThreadId.make("thread-current-store");
      const turnId = TurnId.make("turn-current-store");
      const messageId = MessageId.make("assistant:current-store");
      const commandId = CommandId.make(`provider:${runtimeEventId}:message-complete:${messageId}`);
      const metadata = {
        providerRuntimeMessage: {
          runtimeEventId,
          eventType: "item.completed" as const,
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerTurnId: turnId,
          providerItemId: RuntimeItemId.make("item-current-store"),
        },
      };
      const appended = yield* eventStore.append({
        type: "thread.message-sent",
        eventId: EventId.make("stored-event-current"),
        aggregateKind: "thread",
        aggregateId: threadId,
        occurredAt: now,
        commandId,
        causationEventId: null,
        correlationId: commandId,
        metadata,
        payload: {
          threadId,
          messageId,
          role: "assistant",
          text: "current result",
          turnId,
          streaming: false,
          createdAt: now,
          updatedAt: now,
        },
      });
      assert.deepStrictEqual(appended.metadata, metadata);
      assert.deepStrictEqual(
        yield* sql`
          SELECT typeof(payload_json) AS "payloadStorageClass", payload_json AS "payloadSource",
            typeof(metadata_json) AS "metadataStorageClass", metadata_json AS "metadataSource"
          FROM main.orchestration_events WHERE event_id=${appended.eventId}
        `,
        [
          {
            payloadStorageClass: "text",
            payloadSource:
              '{"threadId":"thread-current-store","messageId":"assistant:current-store","role":"assistant","text":"current result","turnId":"turn-current-store","streaming":false,"createdAt":"2026-08-28T10:00:00.000Z","updatedAt":"2026-08-28T10:00:00.000Z"}',
            metadataStorageClass: "text",
            metadataSource:
              '{"providerRuntimeMessage":{"runtimeEventId":"event-current-store","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-current-store","providerItemId":"item-current-store"}}',
          },
        ],
      );
    }),
  );

  it.effect("stores json columns as strings and replays CLI-origin events", () =>
    Effect.gen(function* () {
      const eventStore = yield* OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-01-01T00:00:00.000Z";

      const appended = yield* eventStore.append({
        type: "project.created",
        eventId: EventId.make("evt-store-roundtrip"),
        aggregateKind: "project",
        aggregateId: ProjectId.make("project-roundtrip"),
        occurredAt: now,
        commandId: CommandId.make("cmd-store-roundtrip"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-store-roundtrip"),
        metadata: {
          adapterKey: "codex",
          origin: {
            surface: "cli",
          },
        },
        payload: {
          projectId: ProjectId.make("project-roundtrip"),
          title: "Roundtrip Project",
          workspaceRoot: "/tmp/project-roundtrip",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });

      const storedRows = yield* sql<{
        readonly payloadJson: string;
        readonly metadataJson: string;
      }>`
        SELECT
          payload_json AS "payloadJson",
          metadata_json AS "metadataJson"
        FROM orchestration_events
        WHERE event_id = ${appended.eventId}
      `;
      assert.equal(storedRows.length, 1);
      assert.equal(typeof storedRows[0]?.payloadJson, "string");
      assert.equal(typeof storedRows[0]?.metadataJson, "string");

      const replayed = yield* Stream.runCollect(eventStore.readFromSequence(0, 10)).pipe(
        Effect.map((chunk) => Array.from(chunk)),
      );
      assert.equal(replayed.length, 1);
      assert.equal(replayed[0]?.type, "project.created");
      assert.equal(replayed[0]?.metadata.adapterKey, "codex");
      assert.deepEqual(replayed[0]?.metadata.origin, { surface: "cli" });
    }),
  );

  it.effect("fails with PersistenceDecodeError when stored json is invalid", () =>
    Effect.gen(function* () {
      const eventStore = yield* OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-01-01T00:00:00.000Z";

      const invalidRows = yield* sql<{ readonly sequence: number }>`
        INSERT INTO orchestration_events (
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
          ${EventId.make("evt-store-invalid-json")},
          ${"project"},
          ${ProjectId.make("project-invalid-json")},
          ${0},
          ${"project.created"},
          ${now},
          ${CommandId.make("cmd-store-invalid-json")},
          ${null},
          ${null},
          ${"server"},
          ${"{"},
          ${"{}"}
        )
        RETURNING sequence
      `;

      const replayResult = yield* Effect.result(
        Stream.runCollect(eventStore.readFromSequence(0, 10)),
      );
      assert.equal(replayResult._tag, "Failure");
      if (replayResult._tag === "Failure") {
        assert.ok(Schema.is(PersistenceDecodeError)(replayResult.failure));
        assert.ok(
          replayResult.failure.operation.includes(
            "OrchestrationEventStore.readFromSequence:decodeRows",
          ),
        );
      }
      const scopedResult = yield* eventStore
        .readAggregateRange({
          aggregateKind: "project",
          aggregateId: "project-invalid-json",
          fromSequenceExclusive: 0,
          toSequenceInclusive: invalidRows[0]!.sequence,
        })
        .pipe(Stream.runCollect, Effect.result);
      assert.equal(scopedResult._tag, "Failure");
      if (scopedResult._tag === "Failure") {
        assert.ok(Schema.is(PersistenceDecodeError)(scopedResult.failure));
        assert.ok(
          scopedResult.failure.operation.includes(
            "OrchestrationEventStore.readAggregateRange:decodeRows",
          ),
        );
      }
    }),
  );

  it.effect("reads one aggregate through the captured head across pruned global gaps", () =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("shared-stream-id");
      const first = yield* store.append(messageEvent(threadId, "scoped-first"));
      const pruned = yield* store.append(
        messageEvent(ThreadId.make("pruned-thread"), "pruned-event"),
      );
      const second = yield* store.append(messageEvent(threadId, "scoped-second"));
      // The same stream ID in a different aggregate is not part of this thread.
      // Its invalid JSON must never reach the event decoder.
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'same-id-project', 'project', ${threadId}, 0, 'project.created',
          '2026-01-01T00:00:00.000Z', 'server', '{', '{'
        ), (
          'unrelated-invalid', 'thread', 'unrelated-invalid-thread', 0, 'thread.activity-appended',
          '2026-01-01T00:00:00.000Z', 'server', '{', '{'
        )
      `;
      const last = yield* store.append(messageEvent(threadId, "scoped-last"));
      yield* sql`DELETE FROM orchestration_events WHERE sequence = ${pruned.sequence}`;
      yield* store.append(messageEvent(threadId, "after-captured-head"));

      const events = yield* store
        .readAggregateRange({
          aggregateKind: "thread",
          aggregateId: threadId,
          fromSequenceExclusive: first.sequence,
          toSequenceInclusive: last.sequence,
          limit: 100,
        })
        .pipe(Stream.runCollect);
      assert.deepEqual(
        events.map((event) => event.sequence),
        [second.sequence, last.sequence],
      );
    }),
  );

  it.effect("bounds thread replay metadata and counts UTF-8 bytes without decoding payloads", () =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{ readonly sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          actor_kind, payload_json, metadata_json
        ) VALUES
          ('stats-1', 'thread', 'stats-thread', 0, 'thread.message-sent',
            '2026-01-01T00:00:00.000Z', 'provider', '{"output":"😀"}', '{}'),
          ('stats-unrelated', 'thread', 'another-thread', 0, 'thread.created',
            '2026-01-01T00:00:00.000Z', 'provider', printf('%.*c', 10000, 'x'), '{}'),
          ('stats-2', 'thread', 'stats-thread', 1, 'thread.activity-appended',
            '2026-01-01T00:00:00.000Z', 'provider', '{', '{}'),
          ('stats-other-kind', 'project', 'stats-thread', 0, 'project.deleted',
            '2026-01-01T00:00:00.000Z', 'provider', printf('%.*c', 20000, 'x'), '{}'),
          ('stats-3', 'thread', 'stats-thread', 2, 'thread.deleted',
            '2026-01-01T00:00:00.000Z', 'provider', '{"output":"é"}', '{}'),
          ('stats-4', 'thread', 'stats-thread', 3, 'thread.created',
            '2026-01-01T00:00:00.000Z', 'provider', printf('%.*c', 2000, 'x'), '{}')
        RETURNING sequence
      `;
      const range = {
        aggregateKind: "thread" as const,
        aggregateId: "stats-thread",
        fromSequenceExclusive: 0,
        toSequenceInclusive: rows.at(-1)!.sequence,
      };
      assert.deepEqual(yield* store.getAggregateReplayStats({ ...range, maxEvents: 2 }), {
        eventCount: 3,
        payloadBytes: 33,
        hasCreateEvent: false,
      });
      assert.deepEqual(yield* store.getAggregateReplayStats({ ...range, maxEvents: 10 }), {
        eventCount: 4,
        payloadBytes: 2033,
        hasCreateEvent: true,
      });
      assert.deepEqual(
        yield* store.getAggregateReplayStats({
          ...range,
          toSequenceInclusive: rows[2]!.sequence,
          maxEvents: 10,
        }),
        {
          eventCount: 2,
          payloadBytes: 18,
          hasCreateEvent: false,
        },
      );
    }),
  );

  it.effect("keeps later pages below the captured head when new events are appended", () =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore;
      const threadId = ThreadId.make("paged-thread");
      const persisted = yield* Effect.forEach(
        Array.from({ length: 502 }, (_, index) => index),
        (index) => store.append(messageEvent(threadId, `paged-${index}`)),
      );
      const head = persisted.at(-1)!.sequence;
      let appendedDuringReplay = false;
      const replayed = yield* store
        .readAggregateRange({
          aggregateKind: "thread",
          aggregateId: threadId,
          fromSequenceExclusive: 0,
          toSequenceInclusive: head,
          limit: 1_000,
        })
        .pipe(
          Stream.tap(() => {
            if (appendedDuringReplay) return Effect.void;
            appendedDuringReplay = true;
            return store.append(messageEvent(threadId, "appended-during-replay"));
          }),
          Stream.runCollect,
        );
      assert.deepEqual(
        replayed.map((event) => event.sequence),
        persisted.map((event) => event.sequence),
      );
      const limited = store.readFromSequence(persisted[0]!.sequence, 501.9);
      for (let run = 0; run < 2; run++) {
        assert.deepEqual(
          (yield* Stream.runCollect(limited)).map((event) => event.sequence),
          persisted.slice(1).map((event) => event.sequence),
        );
      }
      assert.deepEqual(yield* Stream.runCollect(store.readFromSequence(0, -1)), []);
    }),
  );
});

it.live(
  "replays exact historical runtimeEventType metadata after migration without rewriting bytes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-historical-correlation-event-store-",
        });
        const filename = path.join(directory, "state.sqlite");
        const scope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
        const sql = Context.get(context, SqlClient.SqlClient);
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        const metadata =
          '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}';
        const payload =
          '{"threadId":"thread-historical","messageId":"assistant:historical","role":"assistant","text":"historical result","turnId":"turn-historical","streaming":false,"createdAt":"2026-08-26T08:00:00.000Z","updatedAt":"2026-08-26T08:00:00.000Z"}';
        const commandId = "provider:event-historical:message-complete:assistant:historical";
        const metadataWithItem =
          '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":"item-historical"}}';
        const payloadWithItem =
          '{"threadId":"thread-historical","messageId":"assistant:historical-item","role":"assistant","text":"historical item result","turnId":"turn-historical-item","streaming":false,"createdAt":"2026-08-26T08:00:01.000Z","updatedAt":"2026-08-26T08:00:01.000Z"}';
        const commandIdWithItem =
          "provider:event-historical-item:message-complete:assistant:historical-item";
        yield* sql`
        INSERT INTO main.orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_kind,
          payload_json, metadata_json
        ) VALUES (
          'stored-event-historical', 'thread', 'thread-historical', 0,
          'thread.message-sent', '2026-08-26T08:00:00.000Z', ${commandId}, NULL,
          ${commandId}, 'provider', ${payload}, ${metadata}
        )
      `;
        yield* sql`
        INSERT INTO main.orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_kind,
          payload_json, metadata_json
        ) VALUES (
          'stored-event-historical-item', 'thread', 'thread-historical', 1,
          'thread.message-sent', '2026-08-26T08:00:01.000Z', ${commandIdWithItem}, NULL,
          ${commandIdWithItem}, 'provider', ${payloadWithItem}, ${metadataWithItem}
        )
      `;
        const before = yield* sql<Record<string, unknown>>`
        SELECT typeof(payload_json) AS "payloadType",
          hex(CAST(payload_json AS BLOB)) AS "payloadHex",
          typeof(metadata_json) AS "metadataType",
          hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
        FROM main.orchestration_events WHERE event_id LIKE 'stored-event-historical%'
        ORDER BY sequence
      `;
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
          [[60, "AgentControlVerificationEvaluation"]],
        );
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
          SELECT typeof(payload_json) AS "payloadType",
            hex(CAST(payload_json AS BLOB)) AS "payloadHex",
            typeof(metadata_json) AS "metadataType",
            hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
          FROM main.orchestration_events WHERE event_id LIKE 'stored-event-historical%'
          ORDER BY sequence
        `,
          before,
        );
        const storeContext = yield* Layer.buildWithScope(
          OrchestrationEventStoreLive.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, sql))),
          scope,
        );
        const store = Context.get(storeContext, OrchestrationEventStore);
        const replayed = Array.from(yield* Stream.runCollect(store.readFromSequence(0, 10)));
        assert.lengthOf(replayed, 2);
        assert.deepStrictEqual(replayed[0]?.metadata.providerRuntimeMessage, {
          runtimeEventId: EventId.make("event-historical"),
          eventType: "item.completed",
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerTurnId: TurnId.make("turn-historical"),
          providerItemId: null,
        });
        assert.deepStrictEqual(replayed[1]?.metadata.providerRuntimeMessage, {
          runtimeEventId: EventId.make("event-historical-item"),
          eventType: "item.completed",
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerTurnId: TurnId.make("turn-historical-item"),
          providerItemId: RuntimeItemId.make("item-historical"),
        });
        const projectionContext = yield* Layer.buildWithScope(
          OrchestrationProjectionPipelineLive.pipe(
            Layer.provideMerge(Layer.succeed(OrchestrationEventStore, store)),
            Layer.provideMerge(ServerConfig.layerTest(process.cwd(), directory)),
            Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)),
            Layer.provideMerge(NodeServices.layer),
          ),
          scope,
        );
        yield* Context.get(projectionContext, OrchestrationProjectionPipeline).bootstrap;
        assert.deepStrictEqual(
          yield* sql`
          SELECT message_id AS "messageId", thread_id AS "threadId", turn_id AS "turnId",
            text, is_streaming AS "isStreaming"
          FROM main.projection_thread_messages WHERE message_id LIKE 'assistant:historical%'
          ORDER BY message_id
        `,
          [
            {
              messageId: "assistant:historical",
              threadId: "thread-historical",
              turnId: "turn-historical",
              text: "historical result",
              isStreaming: 0,
            },
            {
              messageId: "assistant:historical-item",
              threadId: "thread-historical",
              turnId: "turn-historical-item",
              text: "historical item result",
              isStreaming: 0,
            },
          ],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id=60`,
          [{ migration_id: 60 }],
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("uses the raw duplicate-safe decoder for EventStore replay", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const context = yield* Layer.build(
        OrchestrationEventStoreLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
      );
      const store = Context.get(context, OrchestrationEventStore);
      const sql = Context.get(context, SqlClient.SqlClient);
      const occurredAt = "2026-08-28T10:00:00.000Z";
      const appended = yield* store.append({
        eventId: EventId.make("event-store-duplicate-safe"),
        aggregateKind: "project",
        aggregateId: ProjectId.make("project-store-duplicate-safe"),
        type: "project.created",
        occurredAt,
        commandId: CommandId.make("command-store-duplicate-safe"),
        causationEventId: null,
        correlationId: CommandId.make("command-store-duplicate-safe"),
        metadata: {},
        payload: {
          projectId: ProjectId.make("project-store-duplicate-safe"),
          title: "Duplicate safe",
          workspaceRoot: "/tmp/project-store-duplicate-safe",
          defaultModelSelection: null,
          scripts: [],
          createdAt: occurredAt,
          updatedAt: occurredAt,
        },
      });
      yield* sql`DROP TRIGGER main.agent_control_orchestration_event_update_storage_validate`;
      const duplicatePayload =
        '{"projectId":"project-store-duplicate-safe","title":"first","title":"last","workspaceRoot":"/tmp/project-store-duplicate-safe","defaultModelSelection":null,"scripts":[],"createdAt":"2026-08-28T10:00:00.000Z","updatedAt":"2026-08-28T10:00:00.000Z"}';
      yield* sql`UPDATE main.orchestration_events SET payload_json=${duplicatePayload}
        WHERE event_id=${appended.eventId}`;
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(Stream.runCollect(store.readFromSequence(0, 10)))),
      );
      yield* sql`UPDATE main.orchestration_events SET payload_json=json(payload_json),
        metadata_json=${'{"adapterKey":"codex","adapterKey":"other"}'}
        WHERE event_id=${appended.eventId}`;
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(Stream.runCollect(store.readFromSequence(0, 10)))),
      );
    }),
  ),
);

it.effect("binds productive append and replay statements to MAIN before first prepare", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const sqlContext = yield* Layer.build(SqlitePersistenceMemory);
      const sql = Context.get(sqlContext, SqlClient.SqlClient);
      yield* sql`
        CREATE TEMP TABLE orchestration_events
        AS SELECT * FROM main.orchestration_events WHERE 0
      `;
      yield* sql`ATTACH ':memory:' AS authority_shadow`;
      yield* sql`
        CREATE TABLE authority_shadow.orchestration_events
        AS SELECT * FROM main.orchestration_events WHERE 0
      `;
      const storeContext = yield* Layer.build(
        OrchestrationEventStoreLive.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, sql))),
      );
      const eventStore = Context.get(storeContext, OrchestrationEventStore);
      const now = "2026-01-01T00:00:00.000Z";
      const appended = yield* eventStore.append({
        type: "project.created",
        eventId: EventId.make("evt-store-main-authority"),
        aggregateKind: "project",
        aggregateId: ProjectId.make("project-main-authority"),
        occurredAt: now,
        commandId: CommandId.make("cmd-store-main-authority"),
        causationEventId: null,
        correlationId: CommandId.make("cmd-store-main-authority"),
        metadata: {},
        payload: {
          projectId: ProjectId.make("project-main-authority"),
          title: "Main Authority",
          workspaceRoot: "/tmp/project-main-authority",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });
      assert.equal(appended.eventId, "evt-store-main-authority");
      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT count(*) FROM main.orchestration_events) AS mainCount,
            (SELECT count(*) FROM temp.orchestration_events) AS tempCount,
            (SELECT count(*) FROM authority_shadow.orchestration_events) AS attachedCount
        `,
        [{ mainCount: 1, tempCount: 0, attachedCount: 0 }],
      );
      const replayed = Array.from(yield* Stream.runCollect(eventStore.readFromSequence(0, 10)));
      assert.deepStrictEqual(
        replayed.map((event) => event.eventId),
        ["evt-store-main-authority"],
      );
    }),
  ),
);

for (const reader of ["all", "aggregate"] as const) {
  it.effect(`releases consumed pages during ${reader} replay`, () =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore;
      const threadId = ThreadId.make(`retention-${reader}`);
      yield* Effect.forEach(
        Array.from({ length: 1_501 }, (_, index) => index),
        (index) => store.append(messageEvent(threadId, `retention-${reader}-${index}`)),
        { discard: true },
      );
      // oxlint-disable-next-line typescript/no-extraneous-class -- Identifies page markers for V8's heap query.
      class ReplayPage {}
      let count = 0;
      const replay =
        reader === "all"
          ? store.readAll()
          : store.readAggregateRange({
              aggregateKind: "thread",
              aggregateId: threadId,
              fromSequenceExclusive: 0,
              toSequenceInclusive: 1_501,
              limit: 1_501,
            });
      yield* Stream.runForEach(replay, (event) =>
        Effect.sync(() => {
          assert.equal(event.sequence, count + 1);
          if (count % 500 === 0) {
            // Count live page markers after full GC, without timing or heap-size thresholds.
            Object.assign(event, { replayPage: new ReplayPage() });
            assert.isAtMost(NodeV8.queryObjects(ReplayPage, { format: "count" }), 1);
          }
          count++;
        }),
      );
      assert.equal(count, 1_501);
    }).pipe(
      Effect.provide(OrchestrationEventStoreLive.pipe(Layer.provide(SqlitePersistenceMemory))),
    ),
  );
}
