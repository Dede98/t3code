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

const sameStableBinding = (
  state: AgentControlControlledThreadReservationState,
  payload: AgentControlControlledThreadReservationEvent["payload"],
) =>
  state.controlledThreadReservationId === payload.controlledThreadReservationId &&
  state.threadId === payload.threadId &&
  state.projectId === payload.projectId &&
  state.taskId === payload.taskId &&
  state.taskRevision === payload.taskRevision &&
  state.githubIntakeSequence === payload.githubIntakeSequence &&
  state.sourceIdentityFingerprint === payload.sourceIdentityFingerprint &&
  state.stageRunId === payload.stageRunId &&
  state.attemptId === payload.attemptId &&
  state.roleId === payload.roleId &&
  state.stageKind === payload.stageKind &&
  state.stageOrdinal === payload.stageOrdinal &&
  state.attemptOrdinal === payload.attemptOrdinal &&
  state.leaseId === payload.leaseId &&
  state.fenceToken === payload.fenceToken &&
  state.worktreeReservationId === payload.worktreeReservationId &&
  state.preparedAt === payload.preparedAt;

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
    event.aggregateKind !== "controlled-thread-reservation" ||
    event.aggregateId !== event.payload.controlledThreadReservationId ||
    event.authority !== "controller" ||
    event.causationEventId !== null
  ) {
    return yield* corrupt();
  }

  switch (event.type) {
    case "agentControl.controlledThreadReservation.prepared": {
      if (
        state !== null ||
        event.streamVersion !== 1 ||
        event.commandId !== event.correlationId ||
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
    }
    case "agentControl.controlledThreadReservation.materializing": {
      if (
        state === null ||
        state.status !== "prepared" ||
        event.streamVersion !== 2 ||
        event.commandId !== event.payload.materializingTransitionCommandId ||
        event.correlationId !== event.payload.coordinatorCommandId ||
        event.occurredAt !== event.payload.materializingAt ||
        !sameStableBinding(state, event.payload)
      ) {
        return yield* corrupt();
      }
      return yield* validateAgentControlControlledThreadReservationState({
        schemaVersion: 1,
        ...event.payload,
        revision: 2,
        sequence: event.sequence,
      });
    }
    case "agentControl.controlledThreadReservation.bound": {
      if (
        state === null ||
        state.status !== "materializing" ||
        event.streamVersion !== 3 ||
        event.commandId !== event.payload.boundTransitionCommandId ||
        event.correlationId !== event.payload.coordinatorCommandId ||
        event.occurredAt !== event.payload.boundAt ||
        !sameStableBinding(state, event.payload) ||
        state.coordinatorCommandId !== event.payload.coordinatorCommandId ||
        state.coordinatorCommandFingerprint !== event.payload.coordinatorCommandFingerprint ||
        state.materializingTransitionCommandId !== event.payload.materializingTransitionCommandId ||
        state.materializationCommandId !== event.payload.materializationCommandId ||
        state.materializationCommandFingerprint !==
          event.payload.materializationCommandFingerprint ||
        state.leaseHolderId !== event.payload.leaseHolderId ||
        state.materializingAt !== event.payload.materializingAt
      ) {
        return yield* corrupt();
      }
      return yield* validateAgentControlControlledThreadReservationState({
        schemaVersion: 1,
        ...event.payload,
        revision: 3,
        sequence: event.sequence,
      });
    }
  }
});
