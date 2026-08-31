import type {
  AgentControlRunOnceId,
  AgentControlTaskId,
  AgentControlWorktreeReservationState,
  CommandId,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import type * as Scope from "effect/Scope";

import type { AgentControlWorktreeRpcError } from "@t3tools/contracts";

export interface AgentControlWorktreeControllerShape {
  /**
   * Durable controller saga. Its operation receipt makes retries stable, but
   * deliberately does not claim atomicity between SQLite and Git.
   */
  readonly reserveAndMaterialize: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly taskId: AgentControlTaskId;
  }) => Effect.Effect<AgentControlWorktreeReservationState, AgentControlWorktreeRpcError>;
  readonly reserveAndMaterializeForRunOnce?: (
    runId: AgentControlRunOnceId,
    input: {
      readonly commandId: CommandId;
      readonly projectId: ProjectId;
      readonly taskId: AgentControlTaskId;
    },
  ) => Effect.Effect<AgentControlWorktreeReservationState, AgentControlWorktreeRpcError>;
  readonly reconcile: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly reservationId: AgentControlWorktreeReservationState["reservationId"];
  }) => Effect.Effect<AgentControlWorktreeReservationState, AgentControlWorktreeRpcError>;
  /**
   * Mandatory consumer boundary. `ready` is only a last verified observation;
   * no caller may claim or use a worktree directly from projection state.
   *
   * The callback runs in an owned scope under both repository locks. Attached
   * child fibers are closed before this method returns. Callers must not escape
   * work into detached or foreign scopes.
   */
  readonly useReadyWorktree: <A, E, R>(
    input: {
      readonly projectId: ProjectId;
      readonly reservationId: AgentControlWorktreeReservationState["reservationId"];
    },
    callback: (state: AgentControlWorktreeReservationState) => Effect.Effect<A, E, R>,
    options?: {
      /**
       * Runs after both repository locks are held but before authoritative Git
       * inspection. A completed receipt lets a concurrent loser return without
       * repeating external observation.
       */
      readonly beforeInspection?: Effect.Effect<Option.Option<A>, E, never>;
    },
  ) => Effect.Effect<A, E | AgentControlWorktreeRpcError, Exclude<R, Scope.Scope>>;
  readonly useReadyWorktreeForRunOnce?: <A, E, R>(
    runId: AgentControlRunOnceId,
    input: {
      readonly projectId: ProjectId;
      readonly reservationId: AgentControlWorktreeReservationState["reservationId"];
    },
    callback: (state: AgentControlWorktreeReservationState) => Effect.Effect<A, E, R>,
    options?: { readonly beforeInspection?: Effect.Effect<Option.Option<A>, E, never> },
  ) => Effect.Effect<A, E | AgentControlWorktreeRpcError, Exclude<R, Scope.Scope>>;
}

export class AgentControlWorktreeController extends Context.Service<
  AgentControlWorktreeController,
  AgentControlWorktreeControllerShape
>()("t3/agentControl/worktree/Services/AgentControlWorktreeController") {}
