import {
  AgentControlModeNotAvailableError,
  type AgentControlProjectModeChangedEventDraft,
  type AgentControlProjectState,
  type AgentControlSetProjectModeCommand,
  AgentControlTransitionNotAllowedError,
  type EventId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { AgentControlCommandAuthority } from "./AgentControlCommandAuthority.ts";

export type PlannedAgentControlEvent = AgentControlProjectModeChangedEventDraft;

export function createDefaultAgentControlProjectState(
  projectId: AgentControlProjectState["projectId"],
): AgentControlProjectState {
  return {
    schemaVersion: 1,
    projectId,
    mode: "manual",
    pausedFromMode: null,
    revision: 0,
    sequence: 0,
    updatedAt: null,
  };
}

const isAllowedTransition = (
  from: AgentControlProjectState["mode"],
  to: AgentControlProjectState["mode"],
) =>
  (from === "manual" && to === "observe") ||
  (from === "observe" && (to === "manual" || to === "paused")) ||
  (from === "paused" && (to === "observe" || to === "manual"));

export const decideAgentControlProjectCommand = Effect.fn("decideAgentControlProjectCommand")(
  function* ({
    state,
    command,
    eventId,
    occurredAt,
    authority,
  }: {
    readonly state: AgentControlProjectState;
    readonly command: AgentControlSetProjectModeCommand;
    readonly eventId: EventId;
    readonly occurredAt: string;
    readonly authority: AgentControlCommandAuthority;
  }) {
    if (command.mode === "run-once" || command.mode === "armed") {
      return yield* new AgentControlModeNotAvailableError({
        code: "mode-not-available",
        projectId: command.projectId,
        mode: command.mode,
      });
    }

    if (command.mode === state.mode) {
      return [] as const;
    }

    if (!isAllowedTransition(state.mode, command.mode)) {
      return yield* new AgentControlTransitionNotAllowedError({
        code: "transition-not-allowed",
        projectId: command.projectId,
        fromMode: state.mode,
        toMode: command.mode,
      });
    }

    const pausedFromMode = command.mode === "paused" ? ("observe" as const) : null;
    return [
      {
        eventId,
        type: "agentControl.project.mode.changed",
        aggregateKind: "project-controller",
        aggregateId: command.projectId,
        occurredAt,
        commandId: command.commandId,
        causationEventId: null,
        correlationId: command.commandId,
        authority,
        payload: {
          projectId: command.projectId,
          previousMode: state.mode,
          mode: command.mode,
          previousPausedFromMode: state.pausedFromMode,
          pausedFromMode,
          changedAt: occurredAt,
        },
        metadata: { schemaVersion: 1 },
      },
    ] satisfies ReadonlyArray<PlannedAgentControlEvent>;
  },
);
