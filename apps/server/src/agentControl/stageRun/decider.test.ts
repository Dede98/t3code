import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { decideAgentControlStageRunCommand } from "./decider.ts";
import { deriveAgentControlAttemptId, deriveAgentControlStageRunId } from "./identity.ts";
import { projectAgentControlStageRunEvent } from "./projector.ts";

const at = "2026-07-24T10:00:00.000Z";
const makeCommand = Effect.fn("makeCommand")(function* () {
  const projectId = ProjectId.make("stage-run-project");
  const taskId = AgentControlTaskId.make("stage-run-task");
  const sourceIdentityFingerprint = "a".repeat(64);
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId,
    taskId,
    taskRevision: 2,
    githubIntakeSequence: 3,
    sourceIdentityFingerprint,
    stageKind: "planning",
    stageOrdinal: 1,
  });
  return {
    type: "agentControl.stageRun.prepare" as const,
    commandId: CommandId.make("stage-run-command"),
    projectId,
    taskId,
    stageRunId,
    attemptId: yield* deriveAgentControlAttemptId(stageRunId, 1),
    roleId: AgentControlRoleId.make("planning"),
    stageKind: "planning" as const,
    stageOrdinal: 1,
    attemptOrdinal: 1,
    taskRevision: 2,
    githubIntakeSequence: 3,
    sourceIdentityFingerprint,
    expectedRevision: 0,
  };
});

it.effect("decides and projects only the initial prepared planning stage", () =>
  Effect.gen(function* () {
    const command = yield* makeCommand();
    const drafts = yield* decideAgentControlStageRunCommand({
      state: null,
      command,
      eventId: EventId.make("stage-run-event"),
      occurredAt: at,
    });
    assert.equal(drafts.length, 1);
    const draft = drafts[0]!;
    const state = yield* projectAgentControlStageRunEvent(null, {
      ...draft,
      streamVersion: 1,
      sequence: 7,
    });
    assert.equal(state.status, "prepared");
    assert.equal(state.revision, 1);
    assert.equal(state.sequence, 7);
    const arbitraryStageRunId = AgentControlStageRunId.make("arbitrary-stage-run-id");
    const arbitraryStageRun = yield* Effect.result(
      projectAgentControlStageRunEvent(null, {
        ...draft,
        aggregateId: arbitraryStageRunId,
        payload: { ...draft.payload, stageRunId: arbitraryStageRunId },
        streamVersion: 1,
        sequence: 7,
      }),
    );
    assert.equal(arbitraryStageRun._tag, "Failure");
    const arbitraryAttempt = yield* Effect.result(
      projectAgentControlStageRunEvent(null, {
        ...draft,
        payload: {
          ...draft.payload,
          attemptId: AgentControlAttemptId.make("arbitrary-attempt-id"),
        },
        streamVersion: 1,
        sequence: 7,
      }),
    );
    assert.equal(arbitraryAttempt._tag, "Failure");
    assert.equal(
      (yield* decideAgentControlStageRunCommand({
        state,
        command,
        eventId: EventId.make("stage-run-noop"),
        occurredAt: at,
      })).length,
      0,
    );
  }),
);

it.effect("keeps later status transitions reserved and fail-closed", () =>
  Effect.gen(function* () {
    const command = yield* makeCommand();
    const result = yield* Effect.result(
      decideAgentControlStageRunCommand({
        state: null,
        command: {
          ...command,
          type: "agentControl.stageRun.status.set",
          status: "queued",
        },
        eventId: EventId.make("stage-run-reserved"),
        occurredAt: at,
      }),
    );
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.equal(result.failure.code, "state-not-available");
    }
  }),
);
