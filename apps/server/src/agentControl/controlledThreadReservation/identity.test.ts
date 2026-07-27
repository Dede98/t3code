import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlTaskId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
  lengthFrameAgentControlIdentity,
} from "./identity.ts";

const identity = {
  projectId: ProjectId.make("project-identity"),
  taskId: AgentControlTaskId.make("task-identity"),
  taskRevision: 3,
  githubIntakeSequence: 7,
  sourceIdentityFingerprint: "a".repeat(64),
  stageRunId: AgentControlStageRunId.make("stage-run-identity"),
  attemptId: AgentControlAttemptId.make("attempt-identity"),
  roleId: AgentControlRoleId.make("planning"),
  stageKind: "planning" as const,
  stageOrdinal: 1,
  attemptOrdinal: 1,
};

it.effect("changes both identities for every stable task/stage/source component", () =>
  Effect.gen(function* () {
    const baseReservation = yield* deriveAgentControlControlledThreadReservationId(identity);
    const baseThread = yield* deriveAgentControlReservedThreadId(identity);
    const variants = [
      { ...identity, projectId: ProjectId.make("project-other") },
      { ...identity, taskId: AgentControlTaskId.make("task-other") },
      { ...identity, taskRevision: 4 },
      { ...identity, githubIntakeSequence: 8 },
      { ...identity, sourceIdentityFingerprint: "b".repeat(64) },
      { ...identity, stageRunId: AgentControlStageRunId.make("stage-run-other") },
      { ...identity, attemptId: AgentControlAttemptId.make("attempt-other") },
      { ...identity, roleId: AgentControlRoleId.make("planning-other") },
      { ...identity, stageKind: "verification" as const },
      { ...identity, stageOrdinal: 2 },
      { ...identity, attemptOrdinal: 2 },
    ];
    for (const variant of variants) {
      assert.notEqual(
        yield* deriveAgentControlControlledThreadReservationId(variant),
        baseReservation,
      );
      assert.notEqual(yield* deriveAgentControlReservedThreadId(variant), baseThread);
    }
    assert.match(baseReservation, /^controlled-thread-reservation-[0-9a-f]{64}$/);
    assert.match(baseThread, /^t3-auto-reserved-thread-[0-9a-f]{64}$/);
    assert.notEqual(String(baseReservation), String(baseThread));
  }),
);

it("length framing separates concatenation-equivalent component lists", () => {
  assert.notEqual(
    lengthFrameAgentControlIdentity(["ab", "c"]),
    lengthFrameAgentControlIdentity(["a", "bc"]),
  );
});
