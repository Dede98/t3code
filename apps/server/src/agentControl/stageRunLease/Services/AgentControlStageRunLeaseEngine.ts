import type {
  AgentControlStageRunLeaseCommand,
  AgentControlStageRunLeaseCommandResult,
  AgentControlStageRunLeaseEvent,
  AgentControlStageRunLeaseRpcError,
  AgentControlStageRunLeaseState,
  AgentControlStageRunLeaseView,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";

type WithoutServerFields<T> = T extends AgentControlStageRunLeaseCommand
  ? Omit<T, "authority" | "holderId">
  : never;
export type AgentControlStageRunLeaseDispatchInput =
  WithoutServerFields<AgentControlStageRunLeaseCommand>;

export interface AgentControlStageRunLeaseDispatchCommit {
  readonly result: AgentControlStageRunLeaseCommandResult;
  readonly events: ReadonlyArray<AgentControlStageRunLeaseEvent>;
}

export type AgentControlStageRunLeaseDispatchOutcome =
  | ({ readonly _tag: "Accepted" } & AgentControlStageRunLeaseDispatchCommit)
  | { readonly _tag: "Rejected"; readonly error: AgentControlStageRunLeaseRpcError };

export interface AgentControlStageRunLeaseEngineShape {
  readonly dispatchController: (
    input: AgentControlStageRunLeaseDispatchInput,
  ) => Effect.Effect<AgentControlStageRunLeaseDispatchOutcome, AgentControlStageRunLeaseRpcError>;
  readonly dispatchSystem: (
    input: AgentControlStageRunLeaseDispatchInput,
  ) => Effect.Effect<AgentControlStageRunLeaseDispatchOutcome, AgentControlStageRunLeaseRpcError>;
  readonly toView: (
    state: AgentControlStageRunLeaseState,
  ) => Effect.Effect<AgentControlStageRunLeaseView>;
  readonly rebuild: Effect.Effect<void, AgentControlStageRunLeaseRpcError>;
  readonly streamDomainEvents: Stream.Stream<AgentControlStageRunLeaseEvent>;
}

export class AgentControlStageRunLeaseEngine extends Context.Service<
  AgentControlStageRunLeaseEngine,
  AgentControlStageRunLeaseEngineShape
>()("t3/agentControl/stageRunLease/Services/AgentControlStageRunLeaseEngine") {}
