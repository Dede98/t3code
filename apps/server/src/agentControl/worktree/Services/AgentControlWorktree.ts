import type {
  AgentControlWorktreeGetInput,
  AgentControlWorktreeListInput,
  AgentControlWorktreeListResult,
  AgentControlWorktreeReservationView,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlWorktreeRpcError } from "@t3tools/contracts";

export interface AgentControlWorktreeShape {
  readonly getReservation: (
    input: AgentControlWorktreeGetInput,
  ) => Effect.Effect<AgentControlWorktreeReservationView, AgentControlWorktreeRpcError>;
  readonly listReservations: (
    input: AgentControlWorktreeListInput,
  ) => Effect.Effect<AgentControlWorktreeListResult, AgentControlWorktreeRpcError>;
}

export class AgentControlWorktree extends Context.Service<
  AgentControlWorktree,
  AgentControlWorktreeShape
>()("t3/agentControl/worktree/Services/AgentControlWorktree") {}
