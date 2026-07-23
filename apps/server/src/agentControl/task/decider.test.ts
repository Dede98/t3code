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
import { projectAgentControlTaskEvent } from "./projector.ts";

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
const sourcePrecondition = {
  schemaVersion: 1,
  projectId,
  githubIntakeSequence: 1,
  githubProjectionRevision: 1,
  githubConfigRevision: 1,
  repositoryNodeId: source.repositoryNodeId,
  pollStatus: "success",
  expectedIssueCount: 1,
} as const;
const create: AgentControlTaskCreateFromGithubIssueCommand = {
  type: "agentControl.task.createFromGithubIssue",
  commandId: CommandId.make("task-decider-create"),
  taskId,
  projectId,
  expectedRevision: 0,
  sourcePrecondition,
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
        sourcePrecondition,
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
          sourcePrecondition,
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

it.effect("Agent Control task decider enforces monotone source snapshots", () =>
  Effect.gen(function* () {
    const command = {
      type: "agentControl.task.sourceGate.refresh",
      commandId: CommandId.make("task-decider-monotone"),
      taskId,
      projectId,
      expectedRevision: 1,
      sourcePrecondition,
      source,
      sourceGate: "eligible",
      sourceUpdatedAt: occurredAt,
      githubIntakeSequence: 1,
      sourceSnapshot: snapshot,
    } as const;
    const lowerSequence = yield* Effect.result(
      decideAgentControlTaskCommand({
        state: { ...state, githubIntakeSequence: 2 },
        command: {
          ...command,
          commandId: CommandId.make("task-decider-lower-sequence"),
        },
        eventId: EventId.make("event-task-decider-lower-sequence"),
        occurredAt,
      }),
    );
    assert.equal(lowerSequence._tag, "Failure");
    if (lowerSequence._tag === "Failure") {
      assert.equal(lowerSequence.failure.code, "source-state-conflict");
    }
    const cases = [
      {
        ...command,
        commandId: CommandId.make("task-decider-same-gate-change"),
        sourceGate: "not-ready" as const,
        sourceSnapshot: {
          ...snapshot,
          ready: false,
          eligible: false,
          eligibilityReason: "ready-inactive" as const,
        },
      },
      {
        ...command,
        commandId: CommandId.make("task-decider-same-snapshot-change"),
        sourceSnapshot: { ...snapshot, title: "different untrusted title" },
      },
      {
        ...command,
        commandId: CommandId.make("task-decider-regressed-timestamp"),
        sourcePrecondition: {
          ...sourcePrecondition,
          githubIntakeSequence: 2,
          githubProjectionRevision: 2,
          githubConfigRevision: 2,
        },
        githubIntakeSequence: 2,
        sourceUpdatedAt: "2026-07-23T09:00:00.000Z",
        sourceSnapshot: {
          ...snapshot,
          updatedAt: "2026-07-23T09:00:00.000Z",
        },
      },
    ];
    for (const candidate of cases) {
      const result = yield* Effect.result(
        decideAgentControlTaskCommand({
          state,
          command: candidate,
          eventId: EventId.make(`event-${candidate.commandId}`),
          occurredAt,
        }),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure.code, "source-state-conflict");
      }
    }
  }),
);

it.effect("Agent Control task decider recovers only newer source-missing snapshots", () =>
  Effect.gen(function* () {
    const needsAttention: AgentControlTaskState = {
      ...state,
      status: "needs-attention",
      sourceGate: "source-missing",
      githubIntakeSequence: 2,
      revision: 2,
      sequence: 2,
    };
    const recovered = yield* decideAgentControlTaskCommand({
      state: needsAttention,
      command: {
        type: "agentControl.task.recoverSourceMissing",
        commandId: CommandId.make("task-decider-recover"),
        taskId,
        projectId,
        expectedRevision: 2,
        sourcePrecondition: {
          ...sourcePrecondition,
          githubIntakeSequence: 3,
          githubProjectionRevision: 3,
          githubConfigRevision: 3,
        },
        source,
        sourceGate: "eligible",
        sourceUpdatedAt: occurredAt,
        githubIntakeSequence: 3,
        sourceSnapshot: snapshot,
      },
      eventId: EventId.make("task-decider-recover-event"),
      occurredAt,
    });
    assert.equal(recovered[0]?.type, "agentControl.task.sourceMissingRecovered");

    const identityInvalid = yield* Effect.result(
      decideAgentControlTaskCommand({
        state: { ...needsAttention, sourceGate: "identity-invalid" },
        command: {
          type: "agentControl.task.recoverSourceMissing",
          commandId: CommandId.make("task-decider-identity-no-recover"),
          taskId,
          projectId,
          expectedRevision: 2,
          sourcePrecondition: {
            ...sourcePrecondition,
            githubIntakeSequence: 3,
            githubProjectionRevision: 3,
            githubConfigRevision: 3,
          },
          source,
          sourceGate: "eligible",
          sourceUpdatedAt: occurredAt,
          githubIntakeSequence: 3,
          sourceSnapshot: snapshot,
        },
        eventId: EventId.make("task-decider-identity-no-recover-event"),
        occurredAt,
      }),
    );
    assert.equal(identityInvalid._tag, "Failure");
    if (identityInvalid._tag === "Failure") {
      assert.equal(identityInvalid.failure.code, "source-state-conflict");
    }
  }),
);

it.effect("Agent Control task identity-invalid is an irreversible controller quarantine", () =>
  Effect.gen(function* () {
    const identityInvalid: AgentControlTaskState = {
      ...state,
      status: "needs-attention",
      sourceGate: "identity-invalid",
      githubIntakeSequence: 2,
      revision: 2,
      sequence: 2,
    };
    const baseCommand = {
      type: "agentControl.task.markNeedsAttention",
      commandId: CommandId.make("task-decider-identity-invalid"),
      taskId,
      projectId,
      expectedRevision: 2,
      sourcePrecondition: {
        ...sourcePrecondition,
        githubIntakeSequence: 2,
        githubProjectionRevision: 2,
        githubConfigRevision: 2,
      },
      source,
      sourceUpdatedAt: occurredAt,
      githubIntakeSequence: 2,
      sourceSnapshot: snapshot,
    } as const;

    const identical = yield* decideAgentControlTaskCommand({
      state: identityInvalid,
      command: { ...baseCommand, sourceGate: "identity-invalid" },
      eventId: EventId.make("task-decider-identity-invalid-noop"),
      occurredAt,
    });
    assert.deepStrictEqual(identical, []);

    const sourceMissing = yield* Effect.result(
      decideAgentControlTaskCommand({
        state: identityInvalid,
        command: {
          ...baseCommand,
          commandId: CommandId.make("task-decider-identity-invalid-to-missing"),
          sourceGate: "source-missing",
        },
        eventId: EventId.make("task-decider-identity-invalid-to-missing-event"),
        occurredAt,
      }),
    );
    assert.equal(sourceMissing._tag, "Failure");
    if (sourceMissing._tag === "Failure") {
      assert.equal(sourceMissing.failure.code, "source-state-conflict");
    }

    const newerIdentityInvalid = yield* Effect.result(
      decideAgentControlTaskCommand({
        state: identityInvalid,
        command: {
          ...baseCommand,
          commandId: CommandId.make("task-decider-identity-invalid-newer"),
          sourceGate: "identity-invalid",
          sourcePrecondition: {
            ...baseCommand.sourcePrecondition,
            githubIntakeSequence: 3,
            githubProjectionRevision: 3,
            githubConfigRevision: 3,
          },
          githubIntakeSequence: 3,
        },
        eventId: EventId.make("task-decider-identity-invalid-newer-event"),
        occurredAt,
      }),
    );
    assert.equal(newerIdentityInvalid._tag, "Failure");
  }),
);

it.effect("Agent Control task decider and projector share semantic timestamp ordering", () =>
  Effect.gen(function* () {
    const sameInstantNoop = yield* decideAgentControlTaskCommand({
      state,
      command: {
        type: "agentControl.task.sourceGate.refresh",
        commandId: CommandId.make("task-decider-timestamp-same-instant-noop"),
        taskId,
        projectId,
        expectedRevision: 1,
        sourcePrecondition,
        source,
        sourceGate: "eligible",
        sourceUpdatedAt: "2026-07-23T12:00:00+02:00",
        githubIntakeSequence: 1,
        sourceSnapshot: {
          ...snapshot,
          updatedAt: "2026-07-23T12:00:00+02:00",
        },
      },
      eventId: EventId.make("task-decider-timestamp-same-instant-noop-event"),
      occurredAt,
    });
    assert.deepStrictEqual(sameInstantNoop, []);

    const cases = [
      { name: "same-offset", value: "2026-07-23T12:00:00+02:00", accepted: true },
      { name: "missing-milliseconds", value: "2026-07-23T10:00:00Z", accepted: true },
      { name: "negative-offset", value: "2026-07-23T05:00:00-05:00", accepted: true },
      { name: "semantic-regression", value: "2026-07-23T11:00:00+02:00", accepted: false },
      {
        name: "submillisecond-precision",
        value: "2026-07-23T10:00:00.0001Z",
        accepted: false,
      },
      { name: "invalid-calendar", value: "2026-02-31T10:00:00Z", accepted: false },
      { name: "invalid", value: "not-an-iso-instant", accepted: false },
    ] as const;

    for (const candidate of cases) {
      const candidateSnapshot = { ...snapshot, updatedAt: candidate.value };
      const command = {
        type: "agentControl.task.sourceGate.refresh",
        commandId: CommandId.make(`task-decider-timestamp-${candidate.name}`),
        taskId,
        projectId,
        expectedRevision: 1,
        sourcePrecondition: {
          ...sourcePrecondition,
          githubIntakeSequence: 2,
          githubProjectionRevision: 2,
          githubConfigRevision: 2,
        },
        source,
        sourceGate: "eligible",
        sourceUpdatedAt: candidate.value,
        githubIntakeSequence: 2,
        sourceSnapshot: candidateSnapshot,
      } as const;
      const decided = yield* Effect.result(
        decideAgentControlTaskCommand({
          state,
          command,
          eventId: EventId.make(`task-decider-timestamp-${candidate.name}-event`),
          occurredAt,
        }),
      );
      const projected = yield* Effect.result(
        projectAgentControlTaskEvent(state, {
          eventId: EventId.make(`task-projector-timestamp-${candidate.name}-event`),
          type: "agentControl.task.sourceGate.changed",
          aggregateKind: "task",
          aggregateId: taskId,
          occurredAt,
          commandId: CommandId.make(`task-projector-timestamp-${candidate.name}`),
          causationEventId: null,
          correlationId: CommandId.make(`task-projector-timestamp-${candidate.name}`),
          authority: "controller",
          metadata: { schemaVersion: 1 },
          streamVersion: 2,
          sequence: 2,
          payload: {
            taskId,
            source,
            previousSourceGate: "eligible",
            sourceGate: "eligible",
            sourceUpdatedAt: candidate.value,
            githubIntakeSequence: 2,
            sourceSnapshot: candidateSnapshot,
            changedAt: occurredAt,
          },
        }),
      );
      assert.equal(decided._tag, projected._tag, candidate.name);
      assert.equal(decided._tag === "Success", candidate.accepted, candidate.name);
      if (candidate.name === "submillisecond-precision") {
        if (decided._tag === "Failure") {
          assert.equal(decided.failure.code, "source-state-conflict");
        }
        if (projected._tag === "Failure") {
          assert.equal(projected.failure.code, "projection-corrupt");
        }
      }
      if (decided._tag === "Success" && candidate.accepted) {
        assert.equal(decided.success[0]?.payload.sourceUpdatedAt, occurredAt);
        assert.equal(decided.success[0]?.payload.sourceSnapshot.updatedAt, occurredAt);
      }
    }
  }),
);
