import {
  AgentControlProjectionCorruptError,
  type AgentControlEvent,
  type AgentControlProjectState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

export const AGENT_CONTROL_PROJECT_MODE_PROJECTOR = "agent-control-project-modes-v1";

export function isValidAgentControlProjectState(state: AgentControlProjectState): boolean {
  return (
    (state.mode === "manual" || state.mode === "observe" || state.mode === "paused") &&
    (state.mode === "paused" ? state.pausedFromMode === "observe" : state.pausedFromMode === null)
  );
}

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_PROJECT_MODE_PROJECTOR,
  });

export const projectAgentControlEvent = Effect.fn("projectAgentControlEvent")(function* (
  state: AgentControlProjectState,
  event: AgentControlEvent,
) {
  if (
    !isValidAgentControlProjectState(state) ||
    event.aggregateKind !== "project-controller" ||
    event.aggregateId !== state.projectId ||
    event.payload.projectId !== state.projectId ||
    event.commandId !== event.correlationId ||
    event.causationEventId !== null ||
    event.payload.changedAt !== event.occurredAt ||
    event.streamVersion !== state.revision + 1 ||
    event.sequence <= state.sequence ||
    event.payload.previousMode !== state.mode ||
    event.payload.previousPausedFromMode !== state.pausedFromMode ||
    (event.payload.mode !== "manual" &&
      event.payload.mode !== "observe" &&
      event.payload.mode !== "paused") ||
    (event.payload.mode === "paused"
      ? event.payload.pausedFromMode !== "observe"
      : event.payload.pausedFromMode !== null)
  ) {
    return yield* corrupt();
  }

  return {
    schemaVersion: 1,
    projectId: state.projectId,
    mode: event.payload.mode,
    pausedFromMode: event.payload.pausedFromMode,
    revision: event.streamVersion,
    sequence: event.sequence,
    updatedAt: event.payload.changedAt,
  } satisfies AgentControlProjectState;
});
