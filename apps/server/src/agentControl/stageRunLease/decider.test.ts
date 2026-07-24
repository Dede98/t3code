import {
  AgentControlStageRunLeaseHolderId,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { deriveAgentControlAttemptId, deriveAgentControlStageRunId } from "../stageRun/identity.ts";
import { decideAgentControlStageRunLeaseCommand } from "./decider.ts";
import { deriveAgentControlStageRunLeaseId } from "./identity.ts";
import { projectAgentControlStageRunLeaseEvent } from "./projector.ts";

const at = "2026-07-24T10:00:00.000Z";
const expiresAt = "2026-07-24T10:01:00.000Z";

const fixture = Effect.fn("stageRunLeaseDeciderFixture")(function* () {
  const projectId = ProjectId.make("lease-decider-project");
  const taskId = AgentControlTaskId.make("lease-decider-task");
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
    projectId,
    taskId,
    leaseId,
    stageRunId,
    attemptId,
    sourceIdentityFingerprint,
    holderId: AgentControlStageRunLeaseHolderId.make("holder-decider"),
  };
});

it.effect("reserves token 1, releases, then requires token 2 for a new reservation", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const reserve = {
      type: "agentControl.stageRunLease.reserve" as const,
      commandId: CommandId.make("lease-reserve-1"),
      authority: "controller" as const,
      ...f,
      fenceToken: 1,
      expectedRevision: 0,
      taskRevision: 1,
      githubIntakeSequence: 1,
      leaseDurationMs: 60_000,
    };
    const reservedDraft = (yield* decideAgentControlStageRunLeaseCommand({
      state: null,
      command: reserve,
      eventId: EventId.make("event-reserve-1"),
      occurredAt: at,
      expiresAt,
    }))[0]!;
    const reserved = yield* projectAgentControlStageRunLeaseEvent(null, {
      ...reservedDraft,
      streamVersion: 1,
      sequence: 1,
    });
    assert.equal(reserved.fenceToken, 1);
    assert.equal(reserved.status, "reserved");

    const release = {
      type: "agentControl.stageRunLease.releaseBeforeExecution" as const,
      commandId: CommandId.make("lease-release-1"),
      authority: "controller" as const,
      leaseId: f.leaseId,
      projectId: f.projectId,
      taskId: f.taskId,
      stageRunId: f.stageRunId,
      attemptId: f.attemptId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: f.sourceIdentityFingerprint,
      holderId: f.holderId,
      fenceToken: 1,
      expectedRevision: 1,
    };
    const releasedDraft = (yield* decideAgentControlStageRunLeaseCommand({
      state: reserved,
      command: release,
      eventId: EventId.make("event-release-1"),
      occurredAt: "2026-07-24T10:00:10.000Z",
      expiresAt: null,
    }))[0]!;
    const released = yield* projectAgentControlStageRunLeaseEvent(reserved, {
      ...releasedDraft,
      streamVersion: 2,
      sequence: 2,
    });
    assert.equal(released.status, "released");

    const staleToken = yield* Effect.result(
      decideAgentControlStageRunLeaseCommand({
        state: released,
        command: {
          ...reserve,
          commandId: CommandId.make("lease-reserve-stale"),
          expectedRevision: 2,
        },
        eventId: EventId.make("event-reserve-stale"),
        occurredAt: "2026-07-24T10:00:20.000Z",
        expiresAt: "2026-07-24T10:01:20.000Z",
      }),
    );
    assert.equal(staleToken._tag, "Failure");
    if (staleToken._tag === "Failure") {
      assert.equal(staleToken.failure.code, "fence-token-mismatch");
    }
  }),
);
