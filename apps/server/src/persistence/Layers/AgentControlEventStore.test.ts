import { CommandId, EventId, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlEventStore } from "../Services/AgentControlEventStore.ts";
import { AgentControlEventStoreLive } from "./AgentControlEventStore.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";

const now = "2026-07-22T12:00:00.000Z";

const draft = (
  projectId: string,
  suffix: string,
  previousMode: "manual" | "observe" | "paused" = "manual",
  mode: "manual" | "observe" | "paused" = "observe",
) => ({
  eventId: EventId.make(`event-${suffix}`),
  type: "agentControl.project.mode.changed" as const,
  aggregateKind: "project-controller" as const,
  aggregateId: ProjectId.make(projectId),
  occurredAt: now,
  commandId: CommandId.make(`command-${suffix}`),
  causationEventId: null,
  correlationId: CommandId.make(`command-${suffix}`),
  authority: "human" as const,
  payload: {
    projectId: ProjectId.make(projectId),
    previousMode,
    mode,
    previousPausedFromMode: previousMode === "paused" ? ("observe" as const) : null,
    pausedFromMode: mode === "paused" ? ("observe" as const) : null,
    changedAt: now,
  },
  metadata: { schemaVersion: 1 as const },
});

const layer = it.layer(
  AgentControlEventStoreLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);

layer("AgentControlEventStore", (it) => {
  it.effect("atomically appends batches and replays stream and global order", () =>
    Effect.gen(function* () {
      const store = yield* AgentControlEventStore;
      const first = yield* store.append({
        projectId: ProjectId.make("project-a"),
        expectedStreamVersion: 0,
        events: [draft("project-a", "a1"), draft("project-a", "a2", "observe", "manual")],
      });
      const second = yield* store.append({
        projectId: ProjectId.make("project-b"),
        expectedStreamVersion: 0,
        events: [draft("project-b", "b1")],
      });

      assert.deepStrictEqual(
        first.map((event) => event.streamVersion),
        [1, 2],
      );
      assert.deepStrictEqual(
        [...first, ...second].map((event) => event.sequence),
        [1, 2, 3],
      );
      assert.deepStrictEqual(
        (yield* store.readStream(ProjectId.make("project-a"), 0, 10)).map(
          (event) => event.streamVersion,
        ),
        [1, 2],
      );
      assert.deepStrictEqual(
        (yield* store.readStream(ProjectId.make("project-a"), 0, 1)).map(
          (event) => event.streamVersion,
        ),
        [1],
      );
      assert.deepStrictEqual(
        (yield* store.readStream(ProjectId.make("project-a"), 1, 1)).map(
          (event) => event.streamVersion,
        ),
        [2],
      );
      assert.deepStrictEqual(
        (yield* store.readGlobal(0, 2)).map((event) => event.sequence),
        [1, 2],
      );
      assert.deepStrictEqual(
        (yield* store.readGlobal(2, 2)).map((event) => event.sequence),
        [3],
      );
    }),
  );

  it.effect("returns a typed expected-version conflict", () =>
    Effect.gen(function* () {
      const store = yield* AgentControlEventStore;
      const projectId = ProjectId.make("project-version-conflict");
      yield* store.append({
        projectId,
        expectedStreamVersion: 0,
        events: [draft(projectId, "v1")],
      });
      const result = yield* Effect.result(
        store.append({ projectId, expectedStreamVersion: 0, events: [draft(projectId, "v2")] }),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure._tag, "AgentControlStreamVersionConflictError");
      }
    }),
  );

  it.effect("fails closed for duplicate event ids and corrupt payloads", () =>
    Effect.gen(function* () {
      const store = yield* AgentControlEventStore;
      const firstProject = ProjectId.make("project-duplicate-a");
      const secondProject = ProjectId.make("project-duplicate-b");
      yield* store.append({
        projectId: firstProject,
        expectedStreamVersion: 0,
        events: [draft(firstProject, "duplicate")],
      });
      const duplicate = yield* Effect.result(
        store.append({
          projectId: secondProject,
          expectedStreamVersion: 0,
          events: [draft(secondProject, "duplicate")],
        }),
      );
      assert.equal(duplicate._tag, "Failure");
      if (duplicate._tag === "Failure") {
        assert.equal(duplicate.failure._tag, "AgentControlPersistenceSqlError");
      }
      assert.deepStrictEqual(yield* store.readStream(secondProject, 0, 10), []);

      const batchProject = ProjectId.make("project-atomic-batch");
      const failedBatch = yield* Effect.result(
        store.append({
          projectId: batchProject,
          expectedStreamVersion: 0,
          events: [draft(batchProject, "batch-first"), draft(batchProject, "duplicate")],
        }),
      );
      assert.equal(failedBatch._tag, "Failure");
      assert.deepStrictEqual(yield* store.readStream(batchProject, 0, 10), []);

      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        UPDATE agent_control_events
        SET payload_json = '{broken'
        WHERE event_id = 'event-duplicate'
      `;
      const corrupt = yield* Effect.result(store.readGlobal(0, 10));
      assert.equal(corrupt._tag, "Failure");
      if (corrupt._tag === "Failure") {
        assert.equal(corrupt.failure._tag, "AgentControlPersistenceDecodeError");
      }
    }),
  );
});
