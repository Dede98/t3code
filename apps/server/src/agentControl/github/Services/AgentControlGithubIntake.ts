import type {
  AgentControlGithubClearTrackerConfigInput,
  AgentControlGithubCommandResult,
  AgentControlGithubEvent,
  AgentControlGithubIntakeState,
  AgentControlGithubListIssuesResult,
  AgentControlGithubPollOnceInput,
  AgentControlGithubProjectInput,
  AgentControlGithubRpcError,
  AgentControlGithubSetTrackerConfigInput,
  AgentControlGithubTrackerConfig,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

export interface AgentControlGithubIntakeShape {
  readonly getTrackerConfig: (
    input: AgentControlGithubProjectInput,
  ) => Effect.Effect<AgentControlGithubTrackerConfig | null, AgentControlGithubRpcError>;
  readonly setTrackerConfig: (
    input: AgentControlGithubSetTrackerConfigInput,
  ) => Effect.Effect<AgentControlGithubCommandResult, AgentControlGithubRpcError>;
  readonly clearTrackerConfig: (
    input: AgentControlGithubClearTrackerConfigInput,
  ) => Effect.Effect<AgentControlGithubCommandResult, AgentControlGithubRpcError>;
  readonly getObserveState: (
    input: AgentControlGithubProjectInput,
  ) => Effect.Effect<AgentControlGithubIntakeState, AgentControlGithubRpcError>;
  readonly listObservedIssues: (
    input: AgentControlGithubProjectInput,
  ) => Effect.Effect<AgentControlGithubListIssuesResult, AgentControlGithubRpcError>;
  readonly pollOnce: (
    input: AgentControlGithubPollOnceInput,
  ) => Effect.Effect<AgentControlGithubCommandResult, AgentControlGithubRpcError>;
  /** Hot stream of newly committed events; receipt replays are not emitted. */
  readonly streamDomainEvents: Stream.Stream<AgentControlGithubEvent>;
  /** Acquires a hot subscription before returning the stream. */
  readonly subscribeDomainEvents?: Effect.Effect<
    Stream.Stream<AgentControlGithubEvent>,
    never,
    Scope.Scope
  >;
}

export class AgentControlGithubIntake extends Context.Service<
  AgentControlGithubIntake,
  AgentControlGithubIntakeShape
>()("t3/agentControl/github/Services/AgentControlGithubIntake") {}
