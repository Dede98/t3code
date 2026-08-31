import type {
  AgentControlRunOnceId,
  AgentControlControlledThreadReservationCommandResult,
  AgentControlControlledThreadReservationPrepareInitialInput,
  AgentControlControlledThreadReservationRpcError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

export interface AgentControlControlledThreadActivationShape {
  readonly activateInitial: (
    input: AgentControlControlledThreadReservationPrepareInitialInput,
  ) => Effect.Effect<
    AgentControlControlledThreadReservationCommandResult,
    AgentControlControlledThreadReservationRpcError
  >;
  readonly activateInitialForRunOnce?: (
    runId: AgentControlRunOnceId,
    input: AgentControlControlledThreadReservationPrepareInitialInput,
  ) => Effect.Effect<
    AgentControlControlledThreadReservationCommandResult,
    AgentControlControlledThreadReservationRpcError
  >;
}

export class AgentControlControlledThreadActivation extends Context.Service<
  AgentControlControlledThreadActivation,
  AgentControlControlledThreadActivationShape
>()(
  "t3/agentControl/controlledThreadReservation/Services/AgentControlControlledThreadActivation",
) {}
