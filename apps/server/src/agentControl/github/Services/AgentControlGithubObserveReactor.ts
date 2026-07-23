import type {
  AgentControlGithubProjectInput,
  AgentControlGithubReactorStatus,
  AgentControlGithubRpcError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

export interface AgentControlGithubObserveReactorShape {
  /**
   * Attaches all event subscriptions and performs the initial persisted-state
   * reconcile before returning.
   */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly getStatus: (
    input: AgentControlGithubProjectInput,
  ) => Effect.Effect<AgentControlGithubReactorStatus, AgentControlGithubRpcError>;
}

export class AgentControlGithubObserveReactor extends Context.Service<
  AgentControlGithubObserveReactor,
  AgentControlGithubObserveReactorShape
>()("t3/agentControl/github/Services/AgentControlGithubObserveReactor") {}
