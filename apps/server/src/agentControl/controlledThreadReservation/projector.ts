import {
  AgentControlProjectionCorruptError,
  type AgentControlControlledThreadReservationEvent,
  type AgentControlControlledThreadReservationState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR,
  validateAgentControlControlledThreadReservationState,
} from "./invariant.ts";

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR,
  });

export const projectAgentControlControlledThreadReservationEvent = Effect.fn(
  "projectAgentControlControlledThreadReservationEvent",
)(function* (
  state: AgentControlControlledThreadReservationState | null,
  event: AgentControlControlledThreadReservationEvent,
): Effect.fn.Return<
  AgentControlControlledThreadReservationState,
  AgentControlProjectionCorruptError
> {
  if (
    state !== null ||
    event.aggregateKind !== "controlled-thread-reservation" ||
    event.type !== "agentControl.controlledThreadReservation.prepared" ||
    event.aggregateId !== event.payload.controlledThreadReservationId ||
    event.commandId !== event.correlationId ||
    event.causationEventId !== null ||
    event.authority !== "controller" ||
    event.streamVersion !== 1 ||
    event.occurredAt !== event.payload.preparedAt
  ) {
    return yield* corrupt();
  }
  return yield* validateAgentControlControlledThreadReservationState({
    schemaVersion: 1,
    ...event.payload,
    revision: 1,
    sequence: event.sequence,
  });
});
