import {
  AgentControlProjectionCorruptError,
  type AgentControlStageRunEvent,
  type AgentControlStageRunState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { validateAgentControlStageRunState } from "./initialInvariant.ts";

export const AGENT_CONTROL_STAGE_RUN_PROJECTOR = "agent-control-stage-run-v1";

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_STAGE_RUN_PROJECTOR,
  });

const sameIdentity = (
  state: AgentControlStageRunState,
  payload: AgentControlStageRunEvent["payload"],
) =>
  state.projectId === payload.projectId &&
  state.taskId === payload.taskId &&
  state.stageRunId === payload.stageRunId &&
  state.attemptId === payload.attemptId &&
  state.roleId === payload.roleId &&
  state.stageKind === payload.stageKind &&
  state.stageOrdinal === payload.stageOrdinal &&
  state.attemptOrdinal === payload.attemptOrdinal &&
  state.taskRevision === payload.taskRevision &&
  state.githubIntakeSequence === payload.githubIntakeSequence &&
  state.sourceIdentityFingerprint === payload.sourceIdentityFingerprint;

export const projectAgentControlStageRunEvent = Effect.fn("projectAgentControlStageRunEvent")(
  function* (
    state: AgentControlStageRunState | null,
    event: AgentControlStageRunEvent,
  ): Effect.fn.Return<AgentControlStageRunState, AgentControlProjectionCorruptError> {
    const implementationStarted = event.type === "agentControl.stageRun.implementationStarted";
    const implementationFinalized =
      event.type === "agentControl.stageRun.implementationSucceeded" ||
      event.type === "agentControl.stageRun.implementationFailed" ||
      event.type === "agentControl.stageRun.implementationCancelled";
    if (
      event.aggregateKind !== "stage-run" ||
      event.aggregateId !== event.payload.stageRunId ||
      event.commandId !== event.correlationId ||
      (implementationStarted || implementationFinalized
        ? event.causationEventId === null
        : event.causationEventId !== null) ||
      event.streamVersion !== (state?.revision ?? 0) + 1 ||
      event.sequence <= (state?.sequence ?? 0)
    ) {
      return yield* corrupt();
    }

    if (event.type === "agentControl.stageRun.prepared") {
      if (
        state !== null ||
        event.authority !== "controller" ||
        event.occurredAt !== event.payload.preparedAt
      ) {
        return yield* corrupt();
      }
      return yield* validateAgentControlStageRunState({
        schemaVersion: 1,
        projectId: event.payload.projectId,
        taskId: event.payload.taskId,
        stageRunId: event.payload.stageRunId,
        attemptId: event.payload.attemptId,
        roleId: event.payload.roleId,
        stageKind: event.payload.stageKind,
        stageOrdinal: event.payload.stageOrdinal,
        attemptOrdinal: event.payload.attemptOrdinal,
        status: event.payload.status,
        taskRevision: event.payload.taskRevision,
        githubIntakeSequence: event.payload.githubIntakeSequence,
        sourceIdentityFingerprint: event.payload.sourceIdentityFingerprint,
        createdAt: event.payload.preparedAt,
        updatedAt: event.occurredAt,
        revision: event.streamVersion,
        sequence: event.sequence,
      });
    }

    if (state === null || event.authority !== "system" || !sameIdentity(state, event.payload)) {
      return yield* corrupt();
    }

    if (event.type === "agentControl.stageRun.planningStarted" || implementationStarted) {
      if (state.status !== "prepared" || event.occurredAt !== event.payload.startedAt) {
        return yield* corrupt();
      }
      return yield* validateAgentControlStageRunState({
        ...state,
        status: "running",
        updatedAt: event.occurredAt,
        revision: event.streamVersion,
        sequence: event.sequence,
      });
    }

    const status =
      event.type === "agentControl.stageRun.planningSucceeded" ||
      event.type === "agentControl.stageRun.implementationSucceeded"
        ? "succeeded"
        : event.type === "agentControl.stageRun.planningFailed" ||
            event.type === "agentControl.stageRun.implementationFailed"
          ? "failed"
          : "cancelled";
    if (
      state.status !== "running" ||
      event.payload.status !== status ||
      event.occurredAt !== event.payload.finalizedAt
    ) {
      return yield* corrupt();
    }
    return yield* validateAgentControlStageRunState({
      ...state,
      status,
      updatedAt: event.occurredAt,
      revision: event.streamVersion,
      sequence: event.sequence,
    });
  },
);
