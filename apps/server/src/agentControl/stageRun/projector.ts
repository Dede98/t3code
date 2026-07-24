import {
  AgentControlProjectionCorruptError,
  type AgentControlStageRunEvent,
  type AgentControlStageRunState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

export const AGENT_CONTROL_STAGE_RUN_PROJECTOR = "agent-control-stage-run-v1";

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_STAGE_RUN_PROJECTOR,
  });

export const projectAgentControlStageRunEvent = Effect.fn("projectAgentControlStageRunEvent")(
  function* (
    state: AgentControlStageRunState | null,
    event: AgentControlStageRunEvent,
  ): Effect.fn.Return<AgentControlStageRunState, AgentControlProjectionCorruptError> {
    if (
      state !== null ||
      event.aggregateKind !== "stage-run" ||
      event.type !== "agentControl.stageRun.prepared" ||
      event.aggregateId !== event.payload.stageRunId ||
      event.commandId !== event.correlationId ||
      event.causationEventId !== null ||
      event.authority !== "controller" ||
      event.streamVersion !== 1 ||
      event.payload.preparedAt !== event.occurredAt ||
      event.payload.status !== "prepared" ||
      event.payload.stageKind !== "planning" ||
      event.payload.stageOrdinal !== 1 ||
      event.payload.attemptOrdinal !== 1 ||
      event.payload.roleId !== "planning"
    ) {
      return yield* corrupt();
    }
    return {
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
    };
  },
);
