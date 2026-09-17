import {
  AgentControlEpicRpcError,
  type AgentControlEpicAcceptedResult,
  type AgentControlEpicFinalVerification,
  type AgentControlEpicMemberView,
  type AgentControlVerificationChecks,
  type ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlEpicCaptureInput {
  readonly epicRunId: string;
  readonly projectId: ProjectId;
  readonly taskId: string;
  readonly childRunId: string;
  readonly reservationId: string;
  readonly previousCommitSha: string | null;
  readonly taskFinalizationEvidenceId: string;
}
export interface AgentControlEpicVerifyInput {
  readonly epicRunId: string;
  readonly projectId: ProjectId;
  readonly commitSha: string;
  readonly initialBaseCommitSha: string | null;
  readonly firstAccepted: AgentControlEpicMemberView;
  readonly checks: AgentControlVerificationChecks;
  readonly lastAccepted: AgentControlEpicMemberView;
  /** A failed/incomplete attempt is retained; explicit resume creates a new identity. */
  readonly attempt: number;
}
export interface AgentControlEpicResultHooksShape {
  readonly integrate?: (
    input: AgentControlEpicVerifyInput & {
      readonly captured: AgentControlEpicAcceptedResult;
      readonly expectedCommitSha: string;
      /** Rechecked under the repository lock before publishing any Git result. */
      readonly authorize: Effect.Effect<void, AgentControlEpicRpcError>;
      /** Refresh external scope after checks, outside the final SQLite writer lock. */
      readonly refreshSource?: Effect.Effect<void, AgentControlEpicRpcError>;
      /** Validate reconciled source under the final writer lock without invalidating completed checks. */
      readonly authorizePublication?: Effect.Effect<void, AgentControlEpicRpcError>;
    },
  ) => Effect.Effect<
    {
      readonly accepted: AgentControlEpicAcceptedResult;
      readonly verification: AgentControlEpicFinalVerification;
    },
    AgentControlEpicRpcError
  >;
  readonly capture: (
    input: AgentControlEpicCaptureInput,
  ) => Effect.Effect<AgentControlEpicAcceptedResult, AgentControlEpicRpcError>;
  readonly verify: (
    input: AgentControlEpicVerifyInput,
  ) => Effect.Effect<AgentControlEpicFinalVerification, AgentControlEpicRpcError>;
}
const unavailable = () =>
  Effect.fail(
    new AgentControlEpicRpcError({
      code: "result-service-unavailable",
      message: "Epic result verification is unavailable.",
    }),
  );
export const AgentControlEpicResultHooks = Context.Reference<AgentControlEpicResultHooksShape>(
  "t3/agentControl/epic/ResultHooks",
  {
    defaultValue: () => ({ capture: unavailable, verify: unavailable }),
  },
);
