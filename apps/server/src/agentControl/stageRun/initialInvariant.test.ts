import {
  AgentControlRoleId,
  AgentControlTaskId,
  ProjectId,
  type AgentControlStageRunState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { deriveAgentControlAttemptId, deriveAgentControlStageRunId } from "./identity.ts";
import { validateAgentControlStageRunState } from "./initialInvariant.ts";

const verificationState = Effect.fn("verificationStageRunInvariantFixture")(function* () {
  const projectId = ProjectId.make("verification-stage-invariant-project");
  const taskId = AgentControlTaskId.make("verification-stage-invariant-task");
  const sourceIdentityFingerprint = "a".repeat(64);
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId,
    taskId,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    stageKind: "verification",
    stageOrdinal: 3,
  });
  const attemptId = yield* deriveAgentControlAttemptId(stageRunId, 1);
  return {
    schemaVersion: 1,
    projectId,
    taskId,
    stageRunId,
    attemptId,
    roleId: AgentControlRoleId.make("verifier"),
    stageKind: "verification",
    stageOrdinal: 3,
    attemptOrdinal: 1,
    status: "prepared",
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    createdAt: "2026-08-06T10:00:00.000Z",
    updatedAt: "2026-08-06T10:00:00.000Z",
    revision: 1,
    sequence: 7,
  } satisfies AgentControlStageRunState;
});

it.effect("accepts only verification/verifier/3/1 prepared@1", () =>
  Effect.gen(function* () {
    const prepared = yield* verificationState();
    assert.deepStrictEqual(yield* validateAgentControlStageRunState(prepared), prepared);
    for (const corrupt of [
      { ...prepared, roleId: "planning" },
      { ...prepared, stageOrdinal: 2 },
      { ...prepared, attemptOrdinal: 2 },
      {
        ...prepared,
        status: "running",
        revision: 2,
        updatedAt: "2026-08-06T10:00:01.000Z",
      },
      { ...prepared, revision: 2 },
    ]) {
      assert.equal(
        (yield* Effect.result(validateAgentControlStageRunState(corrupt as never)))._tag,
        "Failure",
      );
    }
  }),
);
