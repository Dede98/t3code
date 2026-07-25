import {
  AgentControlWorktreeReservationId,
  type AgentControlAttemptId,
  type AgentControlStageRunId,
  type AgentControlStageRunLeaseId,
  type AgentControlTaskId,
  type ProjectId,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";

const frame = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`).join("");

export const sha256FramedHex = (parts: ReadonlyArray<string>) =>
  NodeCrypto.createHash("sha256").update(frame(parts), "utf8").digest("hex");

export const deriveAgentControlWorktreeReservationId = (input: {
  readonly projectId: ProjectId;
  readonly taskId: AgentControlTaskId;
  readonly stageRunId: AgentControlStageRunId;
  readonly attemptId: AgentControlAttemptId;
  readonly leaseId: AgentControlStageRunLeaseId;
  readonly fenceToken: number;
  readonly repositoryIdentity: {
    readonly repositoryNodeId: string;
    readonly canonicalKey: string;
  };
  readonly baseCommitSha: string;
}) =>
  Effect.sync(() =>
    AgentControlWorktreeReservationId.make(
      `worktree-reservation-${sha256FramedHex([
        "agent-control-worktree-reservation-v1",
        input.projectId,
        input.taskId,
        input.stageRunId,
        input.attemptId,
        input.leaseId,
        String(input.fenceToken),
        input.repositoryIdentity.repositoryNodeId,
        input.repositoryIdentity.canonicalKey,
        input.baseCommitSha,
      ])}`,
    ),
  );

const MAX_SLUG_LENGTH = 72;

export const sanitizeAgentControlWorktreeSlug = (
  title: string,
  taskId: AgentControlTaskId,
): string => {
  const slug = title
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\p{Cc}/gu, " ")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, MAX_SLUG_LENGTH)
    .replace(/-+$/g, "");
  return slug.length > 0
    ? slug
    : `task-${sha256FramedHex(["agent-control-worktree-slug-fallback-v1", taskId]).slice(0, 12)}`;
};

export const deriveAgentControlWorktreeBranchName = (input: {
  readonly issueNumber: number;
  readonly title: string;
  readonly taskId: AgentControlTaskId;
}) =>
  `t3auto/issue-${input.issueNumber}-${sanitizeAgentControlWorktreeSlug(
    input.title,
    input.taskId,
  )}`;

export const deriveAgentControlWorktreePathKeys = (input: {
  readonly projectId: ProjectId;
  readonly reservationId: AgentControlWorktreeReservationId;
  readonly targetGenerationId: string;
}) => ({
  projectKey: sha256FramedHex(["agent-control-worktree-project-path-v1", input.projectId]).slice(
    0,
    20,
  ),
  reservationKey: sha256FramedHex([
    "agent-control-worktree-reservation-path-v1",
    input.reservationId,
  ]).slice(0, 32),
  generationKey: sha256FramedHex([
    "agent-control-worktree-target-generation-path-v1",
    input.targetGenerationId,
  ]).slice(0, 32),
});
