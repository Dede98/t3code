import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  deriveAgentControlWorktreeBranchName,
  deriveAgentControlWorktreeReservationId,
  sanitizeAgentControlWorktreeSlug,
} from "./identity.ts";

const input = {
  projectId: ProjectId.make("worktree-identity-project"),
  taskId: AgentControlTaskId.make("worktree-identity-task"),
  stageRunId: AgentControlStageRunId.make("worktree-identity-stage"),
  attemptId: AgentControlAttemptId.make("worktree-identity-attempt"),
  leaseId: AgentControlStageRunLeaseId.make("worktree-identity-lease"),
  fenceToken: 7,
  repositoryIdentity: {
    repositoryNodeId: "repository-node",
    canonicalKey: "github.com/owner/repository",
  },
  baseCommitSha: "a".repeat(40),
} as const;

describe("Agent Control worktree identity", () => {
  it.effect("is deterministic and binds lease, fence, base, and stage identity", () =>
    Effect.gen(function* () {
      const id = yield* deriveAgentControlWorktreeReservationId(input);
      assert.equal(id, yield* deriveAgentControlWorktreeReservationId(input));
      assert.notEqual(
        id,
        yield* deriveAgentControlWorktreeReservationId({
          ...input,
          leaseId: AgentControlStageRunLeaseId.make("other-lease"),
        }),
      );
      assert.notEqual(
        id,
        yield* deriveAgentControlWorktreeReservationId({ ...input, fenceToken: 8 }),
      );
      assert.notEqual(
        id,
        yield* deriveAgentControlWorktreeReservationId({
          ...input,
          baseCommitSha: "b".repeat(40),
        }),
      );
      assert.notEqual(
        id,
        yield* deriveAgentControlWorktreeReservationId({
          ...input,
          stageRunId: AgentControlStageRunId.make("other-stage"),
        }),
      );
      assert.notEqual(
        yield* deriveAgentControlWorktreeReservationId({
          ...input,
          repositoryIdentity: { repositoryNodeId: "ab", canonicalKey: "c" },
        }),
        yield* deriveAgentControlWorktreeReservationId({
          ...input,
          repositoryIdentity: { repositoryNodeId: "a", canonicalKey: "bc" },
        }),
      );
    }),
  );

  it("sanitizes untrusted titles into bounded ASCII branch slugs", () => {
    const taskId = input.taskId;
    assert.equal(
      sanitizeAgentControlWorktreeSlug("  Über / .. \\\\ --shell; $(oops)\u0000 ", taskId),
      "uber-shell-oops",
    );
    assert.equal(sanitizeAgentControlWorktreeSlug("a---b___c///d", taskId), "a-b-c-d");
    assert.match(sanitizeAgentControlWorktreeSlug("💣 / .. \u0000", taskId), /^task-[0-9a-f]{12}$/);
    assert.equal(
      sanitizeAgentControlWorktreeSlug("💣", taskId),
      sanitizeAgentControlWorktreeSlug("", taskId),
    );
    assert.isAtMost(sanitizeAgentControlWorktreeSlug("x".repeat(500), taskId).length, 72);
  });

  it("derives the frozen t3auto issue branch format without body or path material", () => {
    const branch = deriveAgentControlWorktreeBranchName({
      issueNumber: 417,
      title: "Fix reconnect / ../../ credentials",
      taskId: input.taskId,
    });
    assert.equal(branch, "t3auto/issue-417-fix-reconnect-credentials");
    assert.notInclude(branch, "..");
    assert.notInclude(branch, "\\");
    assert.notInclude(branch, "/tmp");
  });
});
