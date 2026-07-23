import {
  AgentControlTaskId,
  CommandId,
  type AgentControlTaskSourceIdentity,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";

const frame = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${part.length}:${part}`).join("");

const sha256Hex = (value: string) =>
  NodeCrypto.createHash("sha256").update(value, "utf8").digest("hex");

export const deriveAgentControlTaskId = (
  source: Pick<AgentControlTaskSourceIdentity, "projectId" | "repositoryNodeId" | "issueNodeId">,
) =>
  Effect.sync(() => {
    const digest = sha256Hex(
      frame([source.projectId, source.repositoryNodeId, source.issueNodeId]),
    );
    return AgentControlTaskId.make(`github-${digest}`);
  });

export const deriveAgentControlTaskCommandId = (parts: ReadonlyArray<string>) =>
  Effect.sync(() => {
    const digest = sha256Hex(frame(parts));
    return CommandId.make(`task-intake-${digest}`);
  });
