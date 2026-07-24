import {
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  type AgentControlTaskId,
  type ProjectId,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";

const frame = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`).join("");

const sha256Hex = (parts: ReadonlyArray<string>) =>
  NodeCrypto.createHash("sha256").update(frame(parts), "utf8").digest("hex");

export const deriveAgentControlStageRunLeaseId = (input: {
  readonly projectId: ProjectId;
  readonly taskId: AgentControlTaskId;
}) =>
  Effect.sync(() =>
    AgentControlStageRunLeaseId.make(
      `stage-run-lease-${sha256Hex([
        "agent-control-stage-run-lease-v1",
        input.projectId,
        input.taskId,
      ])}`,
    ),
  );

export const makeAgentControlStageRunLeaseHolderId = (runtimeAttemptId: string) =>
  Effect.sync(() =>
    AgentControlStageRunLeaseHolderId.make(
      `stage-run-lease-holder-${sha256Hex([
        "agent-control-stage-run-lease-holder-v1",
        runtimeAttemptId,
      ])}`,
    ),
  );
