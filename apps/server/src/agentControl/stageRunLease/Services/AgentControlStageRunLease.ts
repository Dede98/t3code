import type {
  AgentControlStageRunLeaseGetInput,
  AgentControlStageRunLeaseListInput,
  AgentControlStageRunLeaseListResult,
  AgentControlStageRunLeaseRpcError,
  AgentControlStageRunLeaseView,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

export interface AgentControlStageRunLeaseShape {
  readonly getLease: (
    input: AgentControlStageRunLeaseGetInput,
  ) => Effect.Effect<AgentControlStageRunLeaseView, AgentControlStageRunLeaseRpcError>;
  readonly listLeases: (
    input: AgentControlStageRunLeaseListInput,
  ) => Effect.Effect<AgentControlStageRunLeaseListResult, AgentControlStageRunLeaseRpcError>;
}

export class AgentControlStageRunLease extends Context.Service<
  AgentControlStageRunLease,
  AgentControlStageRunLeaseShape
>()("t3/agentControl/stageRunLease/Services/AgentControlStageRunLease") {}
