import * as Schema from "effect/Schema";
import { AgentControlEpicSource } from "./agentControlEpic.ts";
import { AgentControlVerificationChecks } from "./agentControl.ts";
import { AgentControlGithubRepositoryBinding } from "./agentControlGithub.ts";

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
  manifestDigest: Schema.optionalKey(Schema.String),
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
export const AgentControlEpicHandoffPullRequest = Schema.Struct({
  number: PositiveInt,
  url: Schema.String,
  state: Schema.Literals(["open", "closed", "merged"]),
  isDraft: Schema.Boolean,
  headSha: Schema.String,
  baseBranch: Schema.String,
});
export type AgentControlEpicHandoffPullRequest = typeof AgentControlEpicHandoffPullRequest.Type;
export const AgentControlEpicHandoff = Schema.Struct({
  intentId: Schema.String,
  status: Schema.Literals(["publishing", "published", "blocked", "failed"]),
  repository: AgentControlGithubRepositoryBinding,
  targetBranch: Schema.String,
  baseCommitSha: Schema.String,
  commitSha: Schema.String,
  branchName: Schema.String,
  verificationEvidenceId: Schema.String,
  branchCreationAttempted: Schema.optionalKey(Schema.Boolean),
  requestedAt: IsoDateTime,
  updatedAt: IsoDateTime,
  pullRequest: Schema.NullOr(AgentControlEpicHandoffPullRequest),
  error: Schema.NullOr(Schema.Struct({ code: Schema.String, message: Schema.String })),
});
export type AgentControlEpicHandoff = typeof AgentControlEpicHandoff.Type;
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
  handoff: Schema.optionalKey(AgentControlEpicHandoff),
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
export const AgentControlEpicHandoffPreviewInput = Schema.Struct({
  projectId: ProjectId,
  epicRunId: Schema.String,
});
export type AgentControlEpicHandoffPreviewInput = typeof AgentControlEpicHandoffPreviewInput.Type;
export const AgentControlEpicHandoffPreview = Schema.Struct({
  ...AgentControlEpicHandoffPreviewInput.fields,
  repository: AgentControlGithubRepositoryBinding,
  targetBranch: Schema.NullOr(Schema.String),
  commitSha: Schema.NullOr(Schema.String),
  branchName: Schema.NullOr(Schema.String),
  canPublish: Schema.Boolean,
  blockers: Schema.Array(AgentControlEpicBlocker),
  handoff: Schema.NullOr(AgentControlEpicHandoff),
});
export type AgentControlEpicHandoffPreview = typeof AgentControlEpicHandoffPreview.Type;
export const AgentControlEpicHandoffPublishInput = Schema.Struct({
  ...AgentControlEpicControlInput.fields,
  expectedCommitSha: Schema.String,
  expectedTargetBranch: Schema.String,
});
export type AgentControlEpicHandoffPublishInput = typeof AgentControlEpicHandoffPublishInput.Type;
export const AGENT_CONTROL_EPIC_RPC_METHODS = {
  preview: "agentControlEpic.preview",
  start: "agentControlEpic.start",
  resume: "agentControlEpic.resume",
  stop: "agentControlEpic.stop",
  clear: "agentControlEpic.clear",
  previewHandoff: "agentControlEpic.previewHandoff",
  publishHandoff: "agentControlEpic.publishHandoff",
} as const;
export class AgentControlEpicRpcError extends Schema.TaggedError<AgentControlEpicRpcError>()(
  "AgentControlEpicRpcError",
  {
    code: Schema.String,
    message: Schema.String,
  },
) {}
