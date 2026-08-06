import {
  AgentControlAttemptId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  ProjectId,
  type AgentControlStageRunLeaseState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { deriveAgentControlAttemptId, deriveAgentControlStageRunId } from "../stageRun/identity.ts";
import { deriveAgentControlStageRunLeaseId } from "./identity.ts";
import { validateAgentControlStageRunLeaseState } from "./invariant.ts";

const fixture = Effect.fn("stageRunLeaseInvariantFixture")(function* () {
  const projectId = ProjectId.make("lease-invariant-project");
  const taskId = AgentControlTaskId.make("lease-invariant-task");
  const sourceIdentityFingerprint = "a".repeat(64);
  const leaseId = yield* deriveAgentControlStageRunLeaseId({ projectId, taskId });
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId,
    taskId,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    stageKind: "planning",
    stageOrdinal: 1,
  });
  const attemptId = yield* deriveAgentControlAttemptId(stageRunId, 1);
  return {
    schemaVersion: 1,
    leaseId,
    projectId,
    taskId,
    stageRunId,
    attemptId,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    holderId: AgentControlStageRunLeaseHolderId.make("lease-invariant-holder"),
    fenceToken: 1,
    status: "reserved",
    acquiredAt: "2026-07-24T10:00:00.000Z",
    renewedAt: "2026-07-24T10:00:00.000Z",
    expiresAt: "2026-07-24T10:01:00.000Z",
    releasedAt: null,
    revision: 1,
    sequence: 1,
  } satisfies AgentControlStageRunLeaseState;
});

it.effect("validates canonical timestamps, hashes, and every derived identity", () =>
  Effect.gen(function* () {
    const valid = yield* fixture();
    assert.deepStrictEqual(yield* validateAgentControlStageRunLeaseState(valid), valid);
    const corruptions = [
      { ...valid, fenceToken: 0 },
      { ...valid, acquiredAt: "2026-07-24T10:00:00Z" },
      { ...valid, expiresAt: valid.renewedAt },
      { ...valid, sourceIdentityFingerprint: "A".repeat(64) },
      {
        ...valid,
        leaseId: AgentControlStageRunLeaseId.make("stage-run-lease-not-derived"),
      },
      {
        ...valid,
        attemptId: AgentControlAttemptId.make(valid.attemptId.replace("attempt-", "attempt-x-")),
      },
    ] as ReadonlyArray<AgentControlStageRunLeaseState>;
    for (const corrupt of corruptions) {
      assert.equal(
        (yield* Effect.result(validateAgentControlStageRunLeaseState(corrupt)))._tag,
        "Failure",
      );
    }
  }),
);

it.effect("reuses the task lease for only a reserved verification epoch with a fresh fence", () =>
  Effect.gen(function* () {
    const planning = yield* fixture();
    const verificationStageRunId = yield* deriveAgentControlStageRunId({
      projectId: planning.projectId,
      taskId: planning.taskId,
      taskRevision: planning.taskRevision,
      githubIntakeSequence: planning.githubIntakeSequence,
      sourceIdentityFingerprint: planning.sourceIdentityFingerprint,
      stageKind: "verification",
      stageOrdinal: 3,
    });
    const verification = {
      ...planning,
      stageRunId: verificationStageRunId,
      attemptId: yield* deriveAgentControlAttemptId(verificationStageRunId, 1),
      fenceToken: 3,
      revision: 5,
      sequence: 11,
    } satisfies AgentControlStageRunLeaseState;
    assert.equal(verification.leaseId, planning.leaseId);
    assert.deepStrictEqual(
      yield* validateAgentControlStageRunLeaseState(verification),
      verification,
    );
    for (const corrupt of [
      { ...verification, fenceToken: 2 },
      {
        ...verification,
        status: "released" as const,
        releasedAt: "2026-07-24T10:00:30.000Z",
      },
    ]) {
      assert.equal(
        (yield* Effect.result(validateAgentControlStageRunLeaseState(corrupt)))._tag,
        "Failure",
      );
    }
  }),
);
