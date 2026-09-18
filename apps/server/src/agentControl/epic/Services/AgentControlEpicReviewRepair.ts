import {
  AgentControlEpicRpcError,
  type AgentControlEpicReviewRepairAttempt,
  type AgentControlEpicReviewRework,
  type AgentControlEpicRuntimeView,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlEpicReviewRepairInput {
  readonly state: AgentControlEpicRuntimeView;
  readonly rework: AgentControlEpicReviewRework;
  /** Rechecked before dispatch and before accepting files from a completed turn. */
  readonly authorize: Effect.Effect<void, AgentControlEpicRpcError>;
}

export type AgentControlEpicReviewRepairProgress =
  | {
      readonly kind: "repairing";
      readonly attempts: ReadonlyArray<AgentControlEpicReviewRepairAttempt>;
    }
  | {
      readonly kind: "candidate";
      readonly attempts: ReadonlyArray<AgentControlEpicReviewRepairAttempt>;
      readonly commitSha: string;
    }
  | {
      readonly kind: "blocked";
      readonly attempts: ReadonlyArray<AgentControlEpicReviewRepairAttempt>;
      readonly code: string;
      readonly message: string;
    };

export class AgentControlEpicReviewRepair extends Context.Reference<{
  readonly progress: (
    input: AgentControlEpicReviewRepairInput,
  ) => Effect.Effect<AgentControlEpicReviewRepairProgress, AgentControlEpicRpcError>;
  readonly cancel: (input: {
    readonly state: AgentControlEpicRuntimeView;
    readonly rework: AgentControlEpicReviewRework;
  }) => Effect.Effect<void, AgentControlEpicRpcError>;
}>("t3/agentControl/epic/ReviewRepair", {
  defaultValue: () => ({
    progress: () =>
      Effect.fail(
        new AgentControlEpicRpcError({
          code: "review-repair-unavailable",
          message: "Epic review repair is unavailable on this server.",
        }),
      ),
    cancel: () => Effect.void,
  }),
}) {}
