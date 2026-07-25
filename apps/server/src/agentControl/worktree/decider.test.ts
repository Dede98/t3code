import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { decideAgentControlWorktreeCommand } from "./decider.ts";
import {
  deriveAgentControlWorktreePathKeys,
  deriveAgentControlWorktreeReservationId,
} from "./identity.ts";
import { projectAgentControlWorktreeEvent } from "./projector.ts";

const at = "2026-07-24T10:00:00.000Z";

const reserveCommand = Effect.fn("worktreeTestReserveCommand")(function* () {
  const projectId = ProjectId.make("worktree-decider-project");
  const taskId = AgentControlTaskId.make("worktree-decider-task");
  const stageRunId = AgentControlStageRunId.make("worktree-decider-stage");
  const attemptId = AgentControlAttemptId.make("worktree-decider-attempt");
  const leaseId = AgentControlStageRunLeaseId.make("worktree-decider-lease");
  const repository = {
    repositoryNodeId: "repository-node",
    nameWithOwner: "owner/repository",
    canonicalKey: "github.com/owner/repository",
    remoteName: "origin",
    remoteUrl: "github.com/owner/repository",
    defaultRemoteRef: "refs/remotes/origin/main",
    commonDirDevice: 1,
    commonDirInode: 1,
  };
  const baseCommitSha = "a".repeat(40);
  const targetGenerationId = "c".repeat(64);
  const reservationId = yield* deriveAgentControlWorktreeReservationId({
    projectId,
    taskId,
    stageRunId,
    attemptId,
    leaseId,
    fenceToken: 3,
    repositoryIdentity: {
      repositoryNodeId: repository.repositoryNodeId,
      canonicalKey: repository.canonicalKey,
    },
    baseCommitSha,
  });
  const keys = deriveAgentControlWorktreePathKeys({
    projectId,
    reservationId,
    targetGenerationId,
  });
  return {
    type: "agentControl.worktree.reserve" as const,
    commandId: CommandId.make("worktree-reserve"),
    reservationId,
    projectId,
    taskId,
    taskRevision: 2,
    githubIntakeSequence: 4,
    sourceIdentityFingerprint: "b".repeat(64),
    stageRunId,
    attemptId,
    leaseId,
    fenceToken: 3,
    expectedRevision: 0,
    repository,
    repositoryWorkspace: "/tmp/repository",
    repositoryCommonDir: "/tmp/repository/.git",
    baseRef: "origin/main",
    baseCommitSha,
    branchName: "t3auto/issue-41-safe-title",
    internalWorktreePath: `/tmp/worktrees/agent-control/project/${keys.reservationKey}-${keys.generationKey}`,
    targetGenerationId,
    worktreeRootDevice: 1,
    worktreeRootInode: 1,
    worktreeParentDevice: 1,
    worktreeParentInode: 1,
  };
});

it.effect("decides and projects the closed reservation lifecycle with revision CAS", () =>
  Effect.gen(function* () {
    const reserve = yield* reserveCommand();
    const reservedDraft = (yield* decideAgentControlWorktreeCommand({
      state: null,
      command: reserve,
      eventId: EventId.make("worktree-event-reserved"),
      occurredAt: at,
    }))[0]!;
    const reserved = yield* projectAgentControlWorktreeEvent(null, {
      ...reservedDraft,
      streamVersion: 1,
      sequence: 100,
    });
    assert.equal(reserved.status, "reserved");

    const start = {
      type: "agentControl.worktree.materialization.start" as const,
      commandId: CommandId.make("worktree-start"),
      reservationId: reserved.reservationId,
      projectId: reserved.projectId,
      taskId: reserved.taskId,
      taskRevision: reserved.taskRevision,
      githubIntakeSequence: reserved.githubIntakeSequence,
      sourceIdentityFingerprint: reserved.sourceIdentityFingerprint,
      stageRunId: reserved.stageRunId,
      attemptId: reserved.attemptId,
      leaseId: reserved.leaseId,
      fenceToken: reserved.fenceToken,
      expectedRevision: 1,
    };
    const startDraft = (yield* decideAgentControlWorktreeCommand({
      state: reserved,
      command: start,
      eventId: EventId.make("worktree-event-start"),
      occurredAt: at,
    }))[0]!;
    const materializing = yield* projectAgentControlWorktreeEvent(reserved, {
      ...startDraft,
      streamVersion: 2,
      sequence: 105,
    });
    assert.equal(materializing.status, "materializing");

    const readyDraft = (yield* decideAgentControlWorktreeCommand({
      state: materializing,
      command: {
        ...start,
        type: "agentControl.worktree.ready",
        commandId: CommandId.make("worktree-ready"),
        expectedRevision: 2,
        headCommitSha: materializing.baseCommitSha,
        ownershipFingerprint: "b".repeat(64),
        gitCreatedDevice: 1,
        gitCreatedInode: 2,
        gitCreatedGitDir: "/tmp/repository/.git/worktrees/reservation",
        markedOwnershipFingerprint: "b".repeat(64),
        verifiedAt: at,
      },
      eventId: EventId.make("worktree-event-ready"),
      occurredAt: at,
    }))[0]!;
    const ready = yield* projectAgentControlWorktreeEvent(materializing, {
      ...readyDraft,
      streamVersion: 3,
      sequence: 110,
    });
    assert.equal(ready.status, "ready");
    assert.equal(ready.headCommitSha, ready.baseCommitSha);

    const stale = yield* Effect.result(
      decideAgentControlWorktreeCommand({
        state: materializing,
        command: { ...start, expectedRevision: 1 },
        eventId: EventId.make("worktree-event-stale"),
        occurredAt: at,
      }),
    );
    assert.equal(stale._tag, "Failure");
    if (stale._tag === "Failure") assert.equal(stale.failure.code, "revision-conflict");
  }),
);

it.effect("projects needs-attention only from a pre-ready state", () =>
  Effect.gen(function* () {
    const reserve = yield* reserveCommand();
    const draft = (yield* decideAgentControlWorktreeCommand({
      state: null,
      command: reserve,
      eventId: EventId.make("worktree-attention-reserve"),
      occurredAt: at,
    }))[0]!;
    const reserved = yield* projectAgentControlWorktreeEvent(null, {
      ...draft,
      streamVersion: 1,
      sequence: 1,
    });
    const attentionDraft = (yield* decideAgentControlWorktreeCommand({
      state: reserved,
      command: {
        type: "agentControl.worktree.needsAttention",
        commandId: CommandId.make("worktree-attention"),
        reservationId: reserved.reservationId,
        projectId: reserved.projectId,
        taskId: reserved.taskId,
        taskRevision: reserved.taskRevision,
        githubIntakeSequence: reserved.githubIntakeSequence,
        sourceIdentityFingerprint: reserved.sourceIdentityFingerprint,
        stageRunId: reserved.stageRunId,
        attemptId: reserved.attemptId,
        leaseId: reserved.leaseId,
        fenceToken: reserved.fenceToken,
        expectedRevision: 1,
        attentionCode: "branch-commit-mismatch",
        materializationPhase: "reserved",
        gitCreatedDevice: null,
        gitCreatedInode: null,
        gitCreatedGitDir: null,
        markedOwnershipFingerprint: null,
      },
      eventId: EventId.make("worktree-attention-event"),
      occurredAt: at,
    }))[0]!;
    const attention = yield* projectAgentControlWorktreeEvent(reserved, {
      ...attentionDraft,
      streamVersion: 2,
      sequence: 2,
    });
    assert.equal(attention.status, "needs-attention");
    assert.equal(attention.attentionCode, "branch-commit-mismatch");
  }),
);
