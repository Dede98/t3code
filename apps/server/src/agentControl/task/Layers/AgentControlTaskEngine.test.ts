import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlTaskEvent,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { AgentControlTaskEngine } from "../Services/AgentControlTaskEngine.ts";

const layer = it.layer(
  AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

const event = {
  eventId: EventId.make("task-engine-publication-original"),
  type: "agentControl.task.created",
  aggregateKind: "task",
  aggregateId: AgentControlTaskId.make("task-engine-publication-task"),
  sequence: 1,
  streamVersion: 1,
  occurredAt: "2026-08-30T12:00:00.000Z",
  commandId: CommandId.make("task-engine-publication-command"),
  causationEventId: null,
  correlationId: CommandId.make("task-engine-publication-command"),
  authority: "controller",
  payload: {
    taskId: AgentControlTaskId.make("task-engine-publication-task"),
    source: {
      projectId: ProjectId.make("task-engine-publication-project"),
      repositoryNodeId: "task-engine-publication-repository",
      issueNodeId: "task-engine-publication-issue",
      issueNumber: 1,
      issueUrl: "https://example.invalid/task-engine-publication/1",
    },
    status: "candidate",
    sourceGate: "eligible",
    stage: "intake",
    sourceUpdatedAt: "2026-08-30T12:00:00.000Z",
    githubIntakeSequence: 1,
    sourceSnapshot: {
      repositoryNodeId: "task-engine-publication-repository",
      issueNodeId: "task-engine-publication-issue",
      number: 1,
      url: "https://example.invalid/task-engine-publication/1",
      state: "open",
      title: "Task Engine publication",
      body: null,
      contentTrust: "untrusted-external",
      updatedAt: "2026-08-30T12:00:00.000Z",
      timelineComplete: true,
      ready: true,
      paused: false,
      eligible: true,
      eligibilityReason: "eligible",
    },
    createdAt: "2026-08-30T12:00:00.000Z",
  },
  metadata: { schemaVersion: 1 },
} satisfies AgentControlTaskEvent;

layer("AgentControlTaskEngine committed publication", (it) => {
  it.effect("publishes each EventId at most once per process runtime", () =>
    Effect.gen(function* () {
      const engine = yield* AgentControlTaskEngine;
      const observed = yield* Ref.make<ReadonlyArray<string>>([]);
      const sentinelSeen = yield* Deferred.make<void>();
      const subscribed = yield* engine.subscribeDomainEvents;
      const listener = yield* Effect.forkChild(
        Stream.runForEach(subscribed, (published) =>
          Ref.update(observed, (current) => [...current, published.eventId]).pipe(
            Effect.andThen(
              published.eventId === "task-engine-publication-sentinel"
                ? Deferred.succeed(sentinelSeen, undefined)
                : Effect.void,
            ),
            Effect.asVoid,
          ),
        ),
      );
      const sentinel = {
        ...event,
        eventId: EventId.make("task-engine-publication-sentinel"),
      };
      const concurrent = {
        ...event,
        eventId: EventId.make("task-engine-publication-concurrent"),
      };

      yield* engine.publishCommitted([event]);
      yield* Effect.all(
        [engine.publishCommitted([concurrent]), engine.publishCommitted([concurrent])],
        { concurrency: "unbounded", discard: true },
      );
      yield* engine.publishCommitted([event, event, sentinel]);
      yield* Deferred.await(sentinelSeen);
      yield* Fiber.interrupt(listener);

      assert.deepStrictEqual(yield* Ref.get(observed), [
        "task-engine-publication-original",
        "task-engine-publication-concurrent",
        "task-engine-publication-sentinel",
      ]);
    }),
  );
});
