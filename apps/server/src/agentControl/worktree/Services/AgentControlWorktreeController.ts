import type {
  AgentControlTaskId,
  AgentControlWorktreeReservationState,
  CommandId,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlWorktreeRpcError } from "@t3tools/contracts";

export interface AgentControlWorktreeControllerShape {
  readonly reserveAndMaterialize: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly taskId: AgentControlTaskId;
  }) => Effect.Effect<AgentControlWorktreeReservationState, AgentControlWorktreeRpcError>;
  readonly reconcile: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly reservationId: AgentControlWorktreeReservationState["reservationId"];
  }) => Effect.Effect<AgentControlWorktreeReservationState, AgentControlWorktreeRpcError>;
}

export class AgentControlWorktreeController extends Context.Service<
  AgentControlWorktreeController,
  AgentControlWorktreeControllerShape
>()("t3/agentControl/worktree/Services/AgentControlWorktreeController") {}
