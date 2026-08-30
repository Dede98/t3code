import type {
  AgentControlTaskCommand,
  AgentControlTaskCommandResult,
  AgentControlTaskEvent,
  AgentControlTaskId,
  AgentControlTaskRpcError,
  AgentControlTaskSourcePrecondition,
  AgentControlTaskState,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

export interface AgentControlTaskEngineShape {
  readonly get: (
    taskId: AgentControlTaskId,
  ) => Effect.Effect<Option.Option<AgentControlTaskState>, AgentControlTaskRpcError>;
  readonly dispatchController: (
    command: AgentControlTaskCommand,
  ) => Effect.Effect<AgentControlTaskCommandResult, AgentControlTaskRpcError>;
  /**
   * Server-internal automatic dispatch. Availability, Observe mode, and the
   * source precondition are checked in the event/projection/receipt transaction.
   */
  readonly dispatchObservedController: (
    command: AgentControlTaskCommand,
  ) => Effect.Effect<AgentControlTaskCommandResult, AgentControlTaskRpcError>;
  readonly verifySourceSnapshot: (
    precondition: AgentControlTaskSourcePrecondition,
  ) => Effect.Effect<void, AgentControlTaskRpcError>;
  readonly rebuild: Effect.Effect<void, AgentControlTaskRpcError>;
  /**
   * Publish task events only after their caller-owned transaction committed.
   * EventIds are idempotent for this engine instance; a fresh runtime may replay
   * durable, incompletely delivered events to newly acquired subscribers.
   */
  readonly publishCommitted: (events: ReadonlyArray<AgentControlTaskEvent>) => Effect.Effect<void>;
  readonly streamDomainEvents: Stream.Stream<AgentControlTaskEvent>;
  readonly subscribeDomainEvents: Effect.Effect<
    Stream.Stream<AgentControlTaskEvent>,
    never,
    Scope.Scope
  >;
}

export class AgentControlTaskEngine extends Context.Service<
  AgentControlTaskEngine,
  AgentControlTaskEngineShape
>()("t3/agentControl/task/Services/AgentControlTaskEngine") {}
