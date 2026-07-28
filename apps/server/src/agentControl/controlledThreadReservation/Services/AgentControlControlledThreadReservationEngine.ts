import type {
  AgentControlControlledThreadReservationCommand,
  AgentControlControlledThreadReservationCommandResult,
  AgentControlControlledThreadReservationEvent,
  AgentControlControlledThreadReservationId,
  AgentControlControlledThreadReservationRpcError,
  AgentControlControlledThreadReservationState,
  AgentControlTaskId,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import type * as Stream from "effect/Stream";

export type AgentControlControlledThreadReservationDispatchOutcome =
  | {
      readonly _tag: "Accepted";
      readonly result: AgentControlControlledThreadReservationCommandResult;
      readonly events: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
    }
  | { readonly _tag: "Rejected"; readonly error: AgentControlControlledThreadReservationRpcError };

export interface AgentControlControlledThreadReservationEngineShape {
  readonly dispatchPreparedController: (
    command: AgentControlControlledThreadReservationCommand,
    commandFingerprint: string,
  ) => Effect.Effect<
    AgentControlControlledThreadReservationDispatchOutcome,
    AgentControlControlledThreadReservationRpcError
  >;
  readonly replayReceiptFirst: (input: {
    readonly commandId: AgentControlControlledThreadReservationCommand["commandId"];
    readonly projectId: ProjectId;
    readonly taskId: AgentControlTaskId;
    readonly commandFingerprint: string;
  }) => Effect.Effect<
    Option.Option<AgentControlControlledThreadReservationCommandResult>,
    AgentControlControlledThreadReservationRpcError
  >;
  readonly getAuthoritative: (
    controlledThreadReservationId: AgentControlControlledThreadReservationId,
  ) => Effect.Effect<
    Option.Option<AgentControlControlledThreadReservationState>,
    AgentControlControlledThreadReservationRpcError
  >;
  readonly validateTaskHistory: (
    projectId: ProjectId,
    taskId: AgentControlTaskId,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlControlledThreadReservationState>,
    AgentControlControlledThreadReservationRpcError
  >;
  readonly refreshCommitted: (
    events: ReadonlyArray<AgentControlControlledThreadReservationEvent>,
  ) => Effect.Effect<void, AgentControlControlledThreadReservationRpcError>;
  readonly publishCommitted: (
    events: ReadonlyArray<AgentControlControlledThreadReservationEvent>,
  ) => Effect.Effect<void>;
  readonly rebuild: Effect.Effect<void, AgentControlControlledThreadReservationRpcError>;
  readonly streamDomainEvents: Stream.Stream<AgentControlControlledThreadReservationEvent>;
}

export class AgentControlControlledThreadReservationEngine extends Context.Service<
  AgentControlControlledThreadReservationEngine,
  AgentControlControlledThreadReservationEngineShape
>()(
  "t3/agentControl/controlledThreadReservation/Services/AgentControlControlledThreadReservationEngine",
) {}
