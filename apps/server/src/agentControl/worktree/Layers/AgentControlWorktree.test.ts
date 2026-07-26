import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  ProjectId,
  type AgentControlWorktreeReservationState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { deriveAgentControlWorktreeReservationId } from "../identity.ts";
import { toAgentControlWorktreeReservationView } from "./AgentControlWorktree.ts";

it.effect("projects a wire view without local paths, holder identity, or untrusted content", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("worktree-view-project");
    const taskId = AgentControlTaskId.make("worktree-view-task");
    const stageRunId = AgentControlStageRunId.make("worktree-view-stage");
    const attemptId = AgentControlAttemptId.make("worktree-view-attempt");
    const leaseId = AgentControlStageRunLeaseId.make("worktree-view-lease");
    const repository = {
      repositoryNodeId: "repository-node-secret-free",
      nameWithOwner: "owner/repository",
      canonicalKey: "github.com/owner/repository",
      remoteName: "origin",
      remoteUrl: "github.com/owner/repository",
      defaultRemoteRef: "refs/remotes/origin/main",
      commonDirDevice: 1,
      commonDirInode: 1,
    };
    const baseCommitSha = "a".repeat(40);
    const state: AgentControlWorktreeReservationState = {
      schemaVersion: 1,
      reservationId: yield* deriveAgentControlWorktreeReservationId({
        projectId,
        taskId,
        stageRunId,
        attemptId,
        leaseId,
        fenceToken: 1,
        repositoryIdentity: {
          repositoryNodeId: repository.repositoryNodeId,
          canonicalKey: repository.canonicalKey,
        },
        baseCommitSha,
      }),
      projectId,
      taskId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: "b".repeat(64),
      stageRunId,
      attemptId,
      leaseId,
      fenceToken: 1,
      repository,
      repositoryWorkspace: "/private/repository",
      repositoryCommonDir: "/private/repository/.git",
      baseRef: "origin/main",
      baseCommitSha,
      branchName: "t3auto/issue-1-safe-title",
      internalWorktreePath: "/private/worktrees/reservation",
      targetGenerationId: "c".repeat(64),
      worktreeRootDevice: 1,
      worktreeRootInode: 1,
      worktreeParentDevice: 1,
      worktreeParentInode: 1,
      materializationPhase: "ownership-marked",
      gitCreatedDevice: 1,
      gitCreatedInode: 2,
      gitCreatedGitDir: "/private/repository/.git/worktrees/reservation",
      markedOwnershipFingerprint: "b".repeat(64),
      status: "ready",
      headCommitSha: baseCommitSha,
      ownershipFingerprint: "b".repeat(64),
      verifiedAt: "2026-07-24T10:00:00.000Z",
      attentionCode: null,
      reservedAt: "2026-07-24T10:00:00.000Z",
      createdAt: "2026-07-24T10:00:00.000Z",
      updatedAt: "2026-07-24T10:00:01.000Z",
      revision: 3,
      sequence: 3,
    };
    const view = toAgentControlWorktreeReservationView(state);
    const serialized = Object.values(view).join("\n");
    assert.notProperty(view, "repositoryWorkspace");
    assert.notProperty(view, "repositoryCommonDir");
    assert.notProperty(view, "internalWorktreePath");
    assert.notProperty(view, "targetGenerationId");
    assert.notProperty(view, "materializationPhase");
    assert.notProperty(view, "gitCreatedDevice");
    assert.notProperty(view, "gitCreatedInode");
    assert.notProperty(view, "gitCreatedGitDir");
    assert.notProperty(view, "markedOwnershipFingerprint");
    assert.notProperty(view, "cause");
    assert.notInclude(serialized, "/private/");
    assert.notInclude(serialized, repository.canonicalKey);
    assert.notInclude(serialized, repository.nameWithOwner);
    assert.notInclude(serialized, "holder");
    assert.notInclude(serialized, "issue body");
  }),
);
