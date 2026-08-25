import { CommandId, EventId, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { OrchestrationEventStore } from "../Services/OrchestrationEventStore.ts";
import { OrchestrationEventStoreLive } from "./OrchestrationEventStore.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";
const layer = it.layer(
  OrchestrationEventStoreLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);

layer("OrchestrationEventStore", (it) => {
  it.effect("stores json columns as strings and replays decoded events", () =>
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
    }),
  );

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
});

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
