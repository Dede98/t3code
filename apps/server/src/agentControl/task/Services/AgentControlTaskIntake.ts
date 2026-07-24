import type {
  AgentControlTaskGetInput,
  AgentControlTaskListInput,
  AgentControlTaskListResult,
  AgentControlTaskReconcileOnceInput,
  AgentControlTaskReconcileOnceResult,
  AgentControlTaskRpcError,
  AgentControlTaskState,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

export interface AgentControlTaskIntakeShape {
  readonly getTask: (
    input: AgentControlTaskGetInput,
  ) => Effect.Effect<AgentControlTaskState, AgentControlTaskRpcError>;
  readonly listTasks: (
    input: AgentControlTaskListInput,
  ) => Effect.Effect<AgentControlTaskListResult, AgentControlTaskRpcError>;
  readonly reconcileOnce: (
    input: AgentControlTaskReconcileOnceInput,
  ) => Effect.Effect<AgentControlTaskReconcileOnceResult, AgentControlTaskRpcError>;
  /** Server-internal automatic path. It is never exposed through RPC. */
  readonly reconcileObservedProject: (
    input: AgentControlTaskReconcileOnceInput,
  ) => Effect.Effect<AgentControlTaskReconcileOnceResult, AgentControlTaskRpcError>;
}

export class AgentControlTaskIntake extends Context.Service<
  AgentControlTaskIntake,
  AgentControlTaskIntakeShape
>()("t3/agentControl/task/Services/AgentControlTaskIntake") {}
