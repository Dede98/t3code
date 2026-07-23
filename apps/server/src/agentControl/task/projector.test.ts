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

it.effect("Agent Control task projector rejects non-monotone source replay", () =>
  Effect.gen(function* () {
    const state = yield* projectAgentControlTaskEvent(null, created);
    const base = {
      eventId: EventId.make("task-projector-monotone"),
      type: "agentControl.task.sourceGate.changed",
      aggregateKind: "task",
      aggregateId: taskId,
      occurredAt: "2026-07-23T11:00:00.000Z",
      commandId: CommandId.make("task-projector-monotone-command"),
      causationEventId: null,
      correlationId: CommandId.make("task-projector-monotone-command"),
      authority: "controller",
      metadata: { schemaVersion: 1 },
      streamVersion: 2,
      sequence: 2,
      payload: {
        taskId,
        source: state.source,
        previousSourceGate: state.sourceGate,
        sourceGate: "eligible",
        sourceUpdatedAt: state.sourceUpdatedAt,
        githubIntakeSequence: 1,
        sourceSnapshot: state.sourceSnapshot,
        changedAt: "2026-07-23T11:00:00.000Z",
      },
    } as const;
    const lowerSequence = yield* Effect.result(
      projectAgentControlTaskEvent(
        { ...state, githubIntakeSequence: 2 },
        {
          ...base,
          eventId: EventId.make("task-projector-lower-sequence"),
        },
      ),
    );
    assert.equal(lowerSequence._tag, "Failure");
    const sameSequence = yield* Effect.result(projectAgentControlTaskEvent(state, base));
    assert.equal(sameSequence._tag, "Failure");

    const differentGate = yield* Effect.result(
      projectAgentControlTaskEvent(state, {
        ...base,
        eventId: EventId.make("task-projector-same-sequence-gate"),
        payload: {
          ...base.payload,
          sourceGate: "not-ready",
          sourceSnapshot: {
            ...base.payload.sourceSnapshot,
            ready: false,
            eligible: false,
            eligibilityReason: "ready-inactive",
          },
        },
      }),
    );
    assert.equal(differentGate._tag, "Failure");

    const regressedTimestamp = yield* Effect.result(
      projectAgentControlTaskEvent(state, {
        ...base,
        eventId: EventId.make("task-projector-regressed-timestamp"),
        payload: {
          ...base.payload,
          githubIntakeSequence: 2,
          sourceUpdatedAt: "2026-07-23T09:00:00.000Z",
          sourceSnapshot: {
            ...base.payload.sourceSnapshot,
            updatedAt: "2026-07-23T09:00:00.000Z",
          },
        },
      }),
    );
    assert.equal(regressedTimestamp._tag, "Failure");
  }),
);

it.effect("Agent Control task projector replays explicit source-missing recovery", () =>
  Effect.gen(function* () {
    const candidate = yield* projectAgentControlTaskEvent(null, created);
    const needsAttention = {
      ...candidate,
      status: "needs-attention" as const,
      sourceGate: "source-missing" as const,
      githubIntakeSequence: 2,
      revision: 2,
      sequence: 2,
    };
    const recovered = yield* projectAgentControlTaskEvent(needsAttention, {
      eventId: EventId.make("task-projector-recovered"),
      type: "agentControl.task.sourceMissingRecovered",
      aggregateKind: "task",
      aggregateId: taskId,
      occurredAt: "2026-07-23T11:00:00.000Z",
      commandId: CommandId.make("task-projector-recovered-command"),
      causationEventId: null,
      correlationId: CommandId.make("task-projector-recovered-command"),
      authority: "controller",
      metadata: { schemaVersion: 1 },
      streamVersion: 3,
      sequence: 3,
      payload: {
        taskId,
        source: needsAttention.source,
        previousStatus: "needs-attention",
        previousSourceGate: "source-missing",
        status: "candidate",
        sourceGate: "eligible",
        sourceUpdatedAt: now,
        githubIntakeSequence: 3,
        sourceSnapshot: needsAttention.sourceSnapshot,
        recoveredAt: "2026-07-23T11:00:00.000Z",
      },
    });
    assert.equal(recovered.status, "candidate");
    assert.equal(recovered.sourceGate, "eligible");
    assert.equal(recovered.githubIntakeSequence, 3);
  }),
);
