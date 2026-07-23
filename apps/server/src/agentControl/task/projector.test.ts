import {
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlTaskEvent,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { projectAgentControlTaskEvent } from "./projector.ts";

const projectId = ProjectId.make("task-projector-project");
const taskId = AgentControlTaskId.make("task-projector");
const now = "2026-07-23T10:00:00.000Z";
const created: AgentControlTaskEvent = {
  eventId: EventId.make("task-projector-created"),
  type: "agentControl.task.created",
  aggregateKind: "task",
  aggregateId: taskId,
  occurredAt: now,
  commandId: CommandId.make("task-projector-command"),
  causationEventId: null,
  correlationId: CommandId.make("task-projector-command"),
  authority: "controller",
  metadata: { schemaVersion: 1 },
  streamVersion: 1,
  sequence: 1,
  payload: {
    taskId,
    source: {
      projectId,
      repositoryNodeId: "repo-node",
      issueNodeId: "issue-node",
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
      issueNodeId: "issue-node",
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

it.effect("Agent Control task projector reconstructs a task and rejects stream gaps", () =>
  Effect.gen(function* () {
    const state = yield* projectAgentControlTaskEvent(null, created);
    assert.equal(state.status, "candidate");
    assert.equal(state.sourceGate, "eligible");
    assert.equal(state.revision, 1);

    const corrupt = yield* Effect.result(
      projectAgentControlTaskEvent(state, {
        ...created,
        eventId: EventId.make("task-projector-gap"),
        streamVersion: 3,
        sequence: 2,
      }),
    );
    assert.equal(corrupt._tag, "Failure");
    if (corrupt._tag === "Failure") {
      assert.equal(corrupt.failure.code, "projection-corrupt");
    }
  }),
);

it.effect("Agent Control task projector changes only the gate for future running tasks", () =>
  Effect.gen(function* () {
    const candidate = yield* projectAgentControlTaskEvent(null, created);
    const running = { ...candidate, status: "running" as const };
    const refreshed = yield* projectAgentControlTaskEvent(running, {
      eventId: EventId.make("task-projector-running-gate"),
      type: "agentControl.task.sourceGate.changed",
      aggregateKind: "task",
      aggregateId: taskId,
      occurredAt: "2026-07-23T11:00:00.000Z",
      commandId: CommandId.make("task-projector-running-gate-command"),
      causationEventId: null,
      correlationId: CommandId.make("task-projector-running-gate-command"),
      authority: "controller",
      metadata: { schemaVersion: 1 },
      streamVersion: 2,
      sequence: 2,
      payload: {
        taskId,
        source: running.source,
        previousSourceGate: "eligible",
        sourceGate: "source-missing",
        sourceUpdatedAt: running.sourceUpdatedAt,
        githubIntakeSequence: 2,
        sourceSnapshot: running.sourceSnapshot,
        changedAt: "2026-07-23T11:00:00.000Z",
      },
    });
    assert.equal(refreshed.status, "running");
    assert.equal(refreshed.sourceGate, "source-missing");
  }),
);
