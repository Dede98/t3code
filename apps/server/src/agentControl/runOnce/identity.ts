import { AgentControlRunOnceId, CommandId, type EventId, type ProjectId } from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";

const frame = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`).join("");

const digest = (domain: string, parts: ReadonlyArray<string>) =>
  NodeCrypto.createHash("sha256")
    .update(frame([domain, ...parts]), "utf8")
    .digest("hex");

export const deriveAgentControlRunOnceId = (input: {
  readonly projectId: ProjectId;
  readonly activationEventId: EventId;
  readonly activationEventSequence: number;
  readonly activationEventStreamVersion: number;
  readonly activationCommandId: CommandId;
}) =>
  AgentControlRunOnceId.make(
    `run-once-${digest("agent-control-run-once-v1", [
      input.projectId,
      input.activationEventId,
      String(input.activationEventSequence),
      String(input.activationEventStreamVersion),
      input.activationCommandId,
    ])}`,
  );

export const deriveRunOnceIdentity = (
  prefix: string,
  domain: string,
  runId: AgentControlRunOnceId,
  ordinal: number,
  ...parts: ReadonlyArray<string>
) =>
  `${prefix}-${digest(`agent-control-run-once-${domain}-v1`, [runId, String(ordinal), ...parts])}`;

export const deriveRunOnceCommandId = (
  runId: AgentControlRunOnceId,
  ordinal: number,
  step: string,
) => CommandId.make(deriveRunOnceIdentity("run-once-command", "command", runId, ordinal, step));

export const deriveRunOnceEvidenceId = (
  runId: AgentControlRunOnceId,
  ordinal: number,
  step: string,
) => deriveRunOnceIdentity("run-once-evidence", "evidence", runId, ordinal, step);

export const deriveRunOnceReceiptId = (
  runId: AgentControlRunOnceId,
  ordinal: number,
  step: string,
) => deriveRunOnceIdentity("run-once-receipt", "receipt", runId, ordinal, step);

export const deriveRunOnceMarkerId = (
  runId: AgentControlRunOnceId,
  ordinal: number,
  step: string,
) => deriveRunOnceIdentity("run-once-marker", "marker", runId, ordinal, step);

export const deriveRunOnceClaimId = (runId: AgentControlRunOnceId, ordinal: number, step: string) =>
  deriveRunOnceIdentity("run-once-claim", "claim", runId, ordinal, step);

export const deriveRunOncePublicationId = (
  runId: AgentControlRunOnceId,
  ordinal: number,
  step: string,
) => deriveRunOnceIdentity("run-once-publication", "publication", runId, ordinal, step);
