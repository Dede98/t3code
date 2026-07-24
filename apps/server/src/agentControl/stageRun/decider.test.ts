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
import { projectAgentControlStageRunEvent } from "./projector.ts";

const command = {
  type: "agentControl.stageRun.prepare" as const,
  commandId: CommandId.make("stage-run-command"),
  projectId: ProjectId.make("stage-run-project"),
  taskId: AgentControlTaskId.make("stage-run-task"),
  stageRunId: AgentControlStageRunId.make("stage-run-id"),
  attemptId: AgentControlAttemptId.make("attempt-id"),
  roleId: AgentControlRoleId.make("planning"),
  stageKind: "planning" as const,
  stageOrdinal: 1,
  attemptOrdinal: 1,
  taskRevision: 2,
  githubIntakeSequence: 3,
  sourceIdentityFingerprint: "fingerprint",
  expectedRevision: 0,
};
const at = "2026-07-24T10:00:00.000Z";

it.effect("decides and projects only the initial prepared planning stage", () =>
  Effect.gen(function* () {
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
