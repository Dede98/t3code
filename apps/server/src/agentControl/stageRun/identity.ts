import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlStageRunId,
  type AgentControlStageKind,
  type AgentControlTaskState,
  type ProjectId,
  type AgentControlTaskId,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";

const frame = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`).join("");

const sha256Hex = (parts: ReadonlyArray<string>) =>
  NodeCrypto.createHash("sha256").update(frame(parts), "utf8").digest("hex");

export const AGENT_CONTROL_INITIAL_STAGE_KIND = "planning" as const;
export const AGENT_CONTROL_INITIAL_STAGE_ORDINAL = 1 as const;
export const AGENT_CONTROL_INITIAL_ATTEMPT_ORDINAL = 1 as const;
export const AGENT_CONTROL_PLANNING_ROLE_ID = AgentControlRoleId.make("planning");

export const deriveAgentControlStageRunId = (input: {
  readonly projectId: ProjectId;
  readonly taskId: AgentControlTaskId;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly stageKind: AgentControlStageKind;
  readonly stageOrdinal: number;
}) =>
  Effect.sync(() =>
    AgentControlStageRunId.make(
      `stage-run-${sha256Hex([
        "agent-control-stage-run-v1",
        input.projectId,
        input.taskId,
        String(input.taskRevision),
        String(input.githubIntakeSequence),
        input.stageKind,
        String(input.stageOrdinal),
      ])}`,
    ),
  );

export const deriveAgentControlAttemptId = (
  stageRunId: AgentControlStageRunId,
  attemptOrdinal: number,
) =>
  Effect.sync(() =>
    AgentControlAttemptId.make(
      `attempt-${sha256Hex([
        "agent-control-stage-run-attempt-v1",
        stageRunId,
        String(attemptOrdinal),
      ])}`,
    ),
  );

/**
 * Fingerprints immutable source identity only. Issue title and body are
 * deliberately excluded; neither is authority or stable identity.
 */
export const deriveAgentControlSourceIdentityFingerprint = (task: AgentControlTaskState) =>
  Effect.sync(() =>
    sha256Hex([
      "agent-control-task-source-identity-v1",
      task.source.projectId,
      task.source.repositoryNodeId,
      task.source.issueNodeId,
      String(task.source.issueNumber),
      task.source.issueUrl,
    ]),
  );

export const deriveRejectedAgentControlStageRunId = (input: {
  readonly projectId: ProjectId;
  readonly taskId: AgentControlTaskId;
}) =>
  Effect.sync(() =>
    AgentControlStageRunId.make(
      `stage-run-rejected-${sha256Hex([
        "agent-control-stage-run-rejected-v1",
        input.projectId,
        input.taskId,
      ])}`,
    ),
  );
