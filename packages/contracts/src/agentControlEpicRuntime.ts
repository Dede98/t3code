import * as Schema from "effect/Schema";
import { AgentControlEpicSource } from "./agentControlEpic.ts";
import { AgentControlVerificationChecks } from "./agentControl.ts";

import {
  AgentControlTaskId,
  CommandId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
} from "./baseSchemas.ts";

export const AgentControlEpicBlocker = Schema.Struct({
  code: Schema.String,
  issueNumber: Schema.NullOr(PositiveInt),
  message: Schema.String,
});
export type AgentControlEpicBlocker = typeof AgentControlEpicBlocker.Type;
export const AgentControlEpicPreview = Schema.Struct({
  projectId: ProjectId,
  source: AgentControlEpicSource,
  canStart: Schema.Boolean,
  blockers: Schema.Array(AgentControlEpicBlocker),
});
export type AgentControlEpicPreview = typeof AgentControlEpicPreview.Type;
export const AgentControlEpicAcceptedResult = Schema.Struct({
  commitSha: Schema.String,
  treeSha: Schema.String,
  codeDigest: Schema.String,
  evidenceId: Schema.String,
});
export type AgentControlEpicAcceptedResult = typeof AgentControlEpicAcceptedResult.Type;
export const AgentControlEpicMemberView = Schema.Struct({
  issueNodeId: Schema.String,
  issueNumber: PositiveInt,
  taskId: Schema.NullOr(AgentControlTaskId),
  childRunId: Schema.NullOr(Schema.String),
  status: Schema.Literals(["pending", "running", "accepted", "external-closed", "failed"]),
  baseCommitSha: Schema.NullOr(Schema.String),
  reservationId: Schema.NullOr(Schema.String),
  taskFinalizationEvidenceId: Schema.NullOr(Schema.String),
  accepted: Schema.NullOr(AgentControlEpicAcceptedResult),
});
export type AgentControlEpicMemberView = typeof AgentControlEpicMemberView.Type;
export const AgentControlEpicFinalVerification = Schema.Struct({
  status: Schema.Literals(["passed", "failed", "blocked"]),
  commitSha: Schema.String,
  evidenceId: Schema.String,
  detail: Schema.String,
  checks: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      command: Schema.String,
      args: Schema.Array(Schema.String),
      cwd: Schema.String,
      required: Schema.Boolean,
      status: Schema.Literals(["passed", "failed", "unavailable", "stale", "missing", "running"]),
      exitCode: Schema.NullOr(Schema.Int),
      output: Schema.NullOr(Schema.String),
      completedAt: Schema.NullOr(IsoDateTime),
    }),
  ),
});
export type AgentControlEpicFinalVerification = typeof AgentControlEpicFinalVerification.Type;
export const AgentControlEpicRuntimeView = Schema.Struct({
  epicRunId: Schema.String,
  projectId: ProjectId,
  revision: NonNegativeInt,
  status: Schema.Literals(["running", "blocked", "verifying", "succeeded", "stopped"]),
  source: AgentControlEpicSource,
  checks: AgentControlVerificationChecks,
  members: Schema.Array(AgentControlEpicMemberView),
  activeTaskId: Schema.NullOr(AgentControlTaskId),
  acceptedCommitSha: Schema.NullOr(Schema.String),
  externalPrerequisites: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        issueNodeId: Schema.String,
        issueNumber: PositiveInt,
        observedAt: IsoDateTime,
      }),
    ),
  ),
  blockers: Schema.Array(AgentControlEpicBlocker),
  blockerHistory: Schema.Array(
    Schema.Struct({
      recordedAt: IsoDateTime,
      blockers: Schema.Array(AgentControlEpicBlocker),
    }),
  ),
  verificationAttempt: PositiveInt,
  finalVerification: Schema.NullOr(AgentControlEpicFinalVerification),
  finalVerificationHistory: Schema.Array(AgentControlEpicFinalVerification),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type AgentControlEpicRuntimeView = typeof AgentControlEpicRuntimeView.Type;
export const AgentControlEpicPreviewInput = Schema.Struct({
  projectId: ProjectId,
  epicNumber: PositiveInt,
});
export type AgentControlEpicPreviewInput = typeof AgentControlEpicPreviewInput.Type;
export const AgentControlEpicStartInput = Schema.Struct({
  ...AgentControlEpicPreviewInput.fields,
  commandId: CommandId,
  expectedRevision: NonNegativeInt,
  expectedFingerprint: Schema.String,
});
export type AgentControlEpicStartInput = typeof AgentControlEpicStartInput.Type;
export const AgentControlEpicControlInput = Schema.Struct({
  projectId: ProjectId,
  commandId: CommandId,
  expectedRevision: NonNegativeInt,
  epicRunId: Schema.String,
});
export type AgentControlEpicControlInput = typeof AgentControlEpicControlInput.Type;
export const AGENT_CONTROL_EPIC_RPC_METHODS = {
  preview: "agentControlEpic.preview",
  start: "agentControlEpic.start",
  resume: "agentControlEpic.resume",
  stop: "agentControlEpic.stop",
  clear: "agentControlEpic.clear",
} as const;
export class AgentControlEpicRpcError extends Schema.TaggedError<AgentControlEpicRpcError>()(
  "AgentControlEpicRpcError",
  {
    code: Schema.String,
    message: Schema.String,
  },
) {}
