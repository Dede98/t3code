import type {
  AgentControlStageRunCommandResult,
  AgentControlStageRunGetInput,
  AgentControlStageRunListInput,
  AgentControlStageRunListResult,
  AgentControlStageRunPrepareInitialInput,
  AgentControlStageRunRpcError,
  AgentControlStageRunState,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

export interface AgentControlStageRunShape {
  readonly getStageRun: (
    input: AgentControlStageRunGetInput,
  ) => Effect.Effect<AgentControlStageRunState, AgentControlStageRunRpcError>;
  readonly listStageRuns: (
    input: AgentControlStageRunListInput,
  ) => Effect.Effect<AgentControlStageRunListResult, AgentControlStageRunRpcError>;
  readonly prepareInitial: (
    input: AgentControlStageRunPrepareInitialInput,
  ) => Effect.Effect<AgentControlStageRunCommandResult, AgentControlStageRunRpcError>;
}

export class AgentControlStageRun extends Context.Service<
  AgentControlStageRun,
  AgentControlStageRunShape
>()("t3/agentControl/stageRun/Services/AgentControlStageRun") {}
