import type {
  AgentControlEvent,
  AgentControlGetProjectStateInput,
  AgentControlProjectState,
  AgentControlRuntimeRpcError,
  AgentControlSetProjectModeInput,
  AgentControlSetProjectModeResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";

export interface AgentControlEngineShape {
  readonly getProjectState: (
    input: AgentControlGetProjectStateInput,
  ) => Effect.Effect<AgentControlProjectState, AgentControlRuntimeRpcError>;
  readonly dispatchHuman: (
    input: AgentControlSetProjectModeInput,
  ) => Effect.Effect<AgentControlSetProjectModeResult, AgentControlRuntimeRpcError>;
  readonly dispatchController: (
    input: AgentControlSetProjectModeInput,
  ) => Effect.Effect<AgentControlSetProjectModeResult, AgentControlRuntimeRpcError>;
  readonly dispatchSystem: (
    input: AgentControlSetProjectModeInput,
  ) => Effect.Effect<AgentControlSetProjectModeResult, AgentControlRuntimeRpcError>;
  readonly streamDomainEvents: Stream.Stream<AgentControlEvent>;
}

export class AgentControlEngine extends Context.Service<
  AgentControlEngine,
  AgentControlEngineShape
>()("t3/agentControl/Services/AgentControlEngine") {}
