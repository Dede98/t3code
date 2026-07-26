import {
  AgentControlProjectionCorruptError,
  type AgentControlControlledThreadReservationState,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "./identity.ts";

export const AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR =
  "agent-control-controlled-thread-reservation-v1";
const CANONICAL_SHA256 = /^[0-9a-f]{64}$/;
const CANONICAL_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

const isCanonicalTimestamp = (value: string) => {
  if (!CANONICAL_TIMESTAMP.test(value)) return false;
  const parsed = DateTime.make(value);
  return Option.isSome(parsed) && DateTime.formatIso(parsed.value) === value;
};

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR,
  });

/**
 * Sole state invariant for decode, decision, projection, replay, direct reads,
 * enumeration/quarantine, save, catch-up, and rebuild.
 */
export const validateAgentControlControlledThreadReservationState = Effect.fn(
  "validateAgentControlControlledThreadReservationState",
)(function* (
  state: AgentControlControlledThreadReservationState,
): Effect.fn.Return<
  AgentControlControlledThreadReservationState,
  AgentControlProjectionCorruptError
> {
  if (
    state.schemaVersion !== 1 ||
    state.stageKind !== "planning" ||
    state.roleId !== "planning" ||
    state.stageOrdinal !== 1 ||
    state.attemptOrdinal !== 1 ||
    state.status !== "prepared" ||
    state.revision !== 1 ||
    state.sequence <= 0 ||
    state.fenceToken <= 0 ||
    !CANONICAL_SHA256.test(state.sourceIdentityFingerprint) ||
    !isCanonicalTimestamp(state.preparedAt)
  ) {
    return yield* corrupt();
  }
  const controlledThreadReservationId =
    yield* deriveAgentControlControlledThreadReservationId(state);
  const threadId = yield* deriveAgentControlReservedThreadId(state);
  if (
    state.controlledThreadReservationId !== controlledThreadReservationId ||
    state.threadId !== threadId
  ) {
    return yield* corrupt();
  }
  return state;
});
