import {
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlTaskCreateFromGithubIssueCommand,
  type AgentControlTaskState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { decideAgentControlTaskCommand } from "./decider.ts";

const projectId = ProjectId.make("task-decider-project");
const taskId = AgentControlTaskId.make("task-decider");
const occurredAt = "2026-07-23T10:00:00.000Z";
const source = {
  projectId,
  repositoryNodeId: "repo-node",
  issueNodeId: "issue-node",
  issueNumber: 42,
  issueUrl: "https://github.test/o/r/issues/42",
} as const;
const snapshot = {
  repositoryNodeId: source.repositoryNodeId,
  issueNodeId: source.issueNodeId,
  number: source.issueNumber,
  url: source.issueUrl,
  state: "open",
  title: "untrusted",
  body: "untrusted body",
  contentTrust: "untrusted-external",
  updatedAt: occurredAt,
  timelineComplete: true,
  ready: true,
  paused: false,
  eligible: true,
  eligibilityReason: "eligible",
} as const;
const create: AgentControlTaskCreateFromGithubIssueCommand = {
  type: "agentControl.task.createFromGithubIssue",
  commandId: CommandId.make("task-decider-create"),
  taskId,
  projectId,
  expectedRevision: 0,
  source,
  sourceGate: "eligible",
  sourceUpdatedAt: occurredAt,
  githubIntakeSequence: 1,
  sourceSnapshot: snapshot,
};
const state: AgentControlTaskState = {
  schemaVersion: 1,
  taskId,
  source,
  status: "candidate",
  sourceGate: "eligible",
  stage: "intake",
  sourceUpdatedAt: occurredAt,
  githubIntakeSequence: 1,
  sourceSnapshot: snapshot,
  createdAt: occurredAt,
  updatedAt: occurredAt,
  revision: 1,
  sequence: 1,
};

it.effect("Agent Control task decider creates once and no-ops identical refreshes", () =>
  Effect.gen(function* () {
    const created = yield* decideAgentControlTaskCommand({
      state: null,
      command: create,
      eventId: EventId.make("task-decider-created"),
      occurredAt,
    });
    assert.equal(created.length, 1);
    assert.equal(created[0]?.type, "agentControl.task.created");

    const refreshed = yield* decideAgentControlTaskCommand({
      state,
      command: {
        type: "agentControl.task.sourceGate.refresh",
        commandId: CommandId.make("task-decider-refresh"),
        taskId,
        projectId,
        expectedRevision: 1,
        source,
        sourceGate: "eligible",
        sourceUpdatedAt: occurredAt,
        githubIntakeSequence: 1,
        sourceSnapshot: snapshot,
      },
      eventId: EventId.make("task-decider-refreshed"),
      occurredAt,
    });
    assert.deepStrictEqual(refreshed, []);
  }),
);

it.effect("Agent Control task decider rejects identity changes and execution states", () =>
  Effect.gen(function* () {
    const identity = yield* Effect.result(
      decideAgentControlTaskCommand({
        state,
        command: {
          type: "agentControl.task.sourceGate.refresh",
          commandId: CommandId.make("task-decider-identity"),
          taskId,
          projectId,
          expectedRevision: 1,
          source: { ...source, issueUrl: "https://attacker.invalid/rebound" },
          sourceGate: "eligible",
          sourceUpdatedAt: occurredAt,
          githubIntakeSequence: 1,
          sourceSnapshot: snapshot,
        },
        eventId: EventId.make("task-decider-identity-event"),
        occurredAt,
      }),
    );
    assert.equal(identity._tag, "Failure");
    if (identity._tag === "Failure") {
      assert.equal(identity.failure.code, "source-identity-conflict");
    }

    const unavailable = yield* Effect.result(
      decideAgentControlTaskCommand({
        state,
        command: {
          type: "agentControl.task.status.set",
          commandId: CommandId.make("task-decider-running"),
          taskId,
          projectId,
          expectedRevision: 1,
          status: "running",
        },
        eventId: EventId.make("task-decider-running-event"),
        occurredAt,
      }),
    );
    assert.equal(unavailable._tag, "Failure");
    if (unavailable._tag === "Failure") {
      assert.equal(unavailable.failure.code, "state-not-available");
    }
  }),
);
