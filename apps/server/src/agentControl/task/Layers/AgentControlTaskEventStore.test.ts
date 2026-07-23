import {
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlTaskEventDraft,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlTaskEventStore } from "../Services/AgentControlTaskEventStore.ts";
import { layer as AgentControlTaskEventStoreLive } from "./AgentControlTaskEventStore.ts";

const layer = it.layer(
  AgentControlTaskEventStoreLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);
const now = "2026-07-23T10:00:00.000Z";

const draft = (taskId: AgentControlTaskId, suffix: string): AgentControlTaskEventDraft => {
  const commandId = CommandId.make(`task-store-command-${suffix}`);
  return {
    eventId: EventId.make(`task-store-event-${suffix}`),
    type: "agentControl.task.created",
    aggregateKind: "task",
    aggregateId: taskId,
    occurredAt: now,
    commandId,
    causationEventId: null,
    correlationId: commandId,
    authority: "controller",
    metadata: { schemaVersion: 1 },
    payload: {
      taskId,
      source: {
        projectId: ProjectId.make("task-store-project"),
        repositoryNodeId: "repo-node",
        issueNodeId: `issue-node-${suffix}`,
        issueNumber: 1,
        issueUrl: "https://github.test/o/r/issues/1",
      },
      status: "candidate",
      sourceGate: "eligible",
      stage: "intake",
      sourceUpdatedAt: now,
      githubIntakeSequence: 1,
      sourceSnapshot: {
        repositoryNodeId: "repo-node",
        issueNodeId: `issue-node-${suffix}`,
        number: 1,
        url: "https://github.test/o/r/issues/1",
        state: "open",
        title: "untrusted",
        body: null,
        contentTrust: "untrusted-external",
        updatedAt: now,
        timelineComplete: true,
        ready: true,
        paused: false,
        eligible: true,
        eligibilityReason: "eligible",
      },
      createdAt: now,
    },
  };
};

layer("AgentControlTaskEventStore", (it) => {
  it.effect("appends, replays, and CAS-rejects task streams", () =>
    Effect.gen(function* () {
      const store = yield* AgentControlTaskEventStore;
      const taskId = AgentControlTaskId.make("task-store");
      const appended = yield* store.append({
        taskId,
        expectedStreamVersion: 0,
        events: [draft(taskId, "one")],
      });
      assert.equal(appended[0]?.streamVersion, 1);
      assert.equal((yield* store.readStream(taskId)).length, 1);
      assert.equal((yield* store.readGlobal()).length, 1);

      const conflict = yield* Effect.result(
        store.append({
          taskId,
          expectedStreamVersion: 0,
          events: [draft(taskId, "two")],
        }),
      );
      assert.equal(conflict._tag, "Failure");
      if (conflict._tag === "Failure") {
        assert.equal(conflict.failure._tag, "AgentControlTaskStreamVersionConflictError");
      }
    }),
  );
});
