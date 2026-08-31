import type {
  AgentControlRunOnceId,
  AgentControlWorktreeCommand,
  AgentControlWorktreeCommandResult,
  AgentControlWorktreeEvent,
  AgentControlWorktreeReservationState,
  AgentControlWorktreeRpcError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

export type AgentControlWorktreeDispatchOutcome =
  | {
      readonly _tag: "Accepted";
      readonly result: AgentControlWorktreeCommandResult;
      readonly events: ReadonlyArray<AgentControlWorktreeEvent>;
    }
  | { readonly _tag: "Rejected"; readonly error: AgentControlWorktreeRpcError };

export interface AgentControlWorktreeEngineShape {
  readonly dispatchController: (
    command: AgentControlWorktreeCommand,
  ) => Effect.Effect<AgentControlWorktreeDispatchOutcome, AgentControlWorktreeRpcError>;
  readonly dispatchControllerForRunOnce?: (
    runId: AgentControlRunOnceId,
    command: AgentControlWorktreeCommand,
  ) => Effect.Effect<AgentControlWorktreeDispatchOutcome, AgentControlWorktreeRpcError>;
  readonly loadAuthoritative: (
    reservationId: AgentControlWorktreeCommand["reservationId"],
  ) => Effect.Effect<AgentControlWorktreeReservationState | null, AgentControlWorktreeRpcError>;
  readonly rebuild: Effect.Effect<void, AgentControlWorktreeRpcError>;
  readonly streamDomainEvents: Stream.Stream<AgentControlWorktreeEvent>;
  readonly subscribeDomainEvents: Effect.Effect<
    Stream.Stream<AgentControlWorktreeEvent>,
    never,
    Scope.Scope
  >;
}

export class AgentControlWorktreeEngine extends Context.Service<
  AgentControlWorktreeEngine,
  AgentControlWorktreeEngineShape
>()("t3/agentControl/worktree/Services/AgentControlWorktreeEngine") {}
