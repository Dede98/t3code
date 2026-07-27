import type {
  AgentControlControlledThreadReservationEvent,
  AgentControlProjectionCorruptError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type {
  AgentControlControlledThreadReservationEventStoreError,
  AgentControlRepositoryError,
} from "../../Errors.ts";

export type AgentControlControlledThreadReservationProjectionError =
  | AgentControlControlledThreadReservationEventStoreError
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError;

export interface AgentControlControlledThreadReservationProjectionShape {
  readonly bootstrap: Effect.Effect<void, AgentControlControlledThreadReservationProjectionError>;
  readonly projectEvent: (
    event: AgentControlControlledThreadReservationEvent,
  ) => Effect.Effect<void, AgentControlControlledThreadReservationProjectionError>;
  readonly rebuild: Effect.Effect<void, AgentControlControlledThreadReservationProjectionError>;
}

export class AgentControlControlledThreadReservationProjection extends Context.Service<
  AgentControlControlledThreadReservationProjection,
  AgentControlControlledThreadReservationProjectionShape
>()(
  "t3/agentControl/controlledThreadReservation/Services/AgentControlControlledThreadReservationProjection",
) {}
