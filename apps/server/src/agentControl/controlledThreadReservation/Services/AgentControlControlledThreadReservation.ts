import type {
  AgentControlControlledThreadReservationCommandResult,
  AgentControlControlledThreadReservationGetInput,
  AgentControlControlledThreadReservationListInput,
  AgentControlControlledThreadReservationListResult,
  AgentControlControlledThreadReservationPrepareInitialInput,
  AgentControlControlledThreadReservationRpcError,
  AgentControlControlledThreadReservationView,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

export interface AgentControlControlledThreadReservationShape {
  readonly get: (
    input: AgentControlControlledThreadReservationGetInput,
  ) => Effect.Effect<
    AgentControlControlledThreadReservationView,
    AgentControlControlledThreadReservationRpcError
  >;
  readonly list: (
    input: AgentControlControlledThreadReservationListInput,
  ) => Effect.Effect<
    AgentControlControlledThreadReservationListResult,
    AgentControlControlledThreadReservationRpcError
  >;
  readonly prepareInitial: (
    input: AgentControlControlledThreadReservationPrepareInitialInput,
  ) => Effect.Effect<
    AgentControlControlledThreadReservationCommandResult,
    AgentControlControlledThreadReservationRpcError
  >;
}

export class AgentControlControlledThreadReservation extends Context.Service<
  AgentControlControlledThreadReservation,
  AgentControlControlledThreadReservationShape
>()(
  "t3/agentControl/controlledThreadReservation/Services/AgentControlControlledThreadReservation",
) {}
