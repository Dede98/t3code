import {
  AgentControlProjectionCorruptError,
  type AgentControlTaskEvent,
  type AgentControlTaskState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

export const AGENT_CONTROL_TASK_PROJECTOR = "agent-control-task-v1";

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_TASK_PROJECTOR,
  });

const sameSource = (
  left: AgentControlTaskState["source"],
  right: AgentControlTaskState["source"],
) =>
  left.projectId === right.projectId &&
  left.repositoryNodeId === right.repositoryNodeId &&
  left.issueNodeId === right.issueNodeId &&
  left.issueNumber === right.issueNumber &&
  left.issueUrl === right.issueUrl;

export const projectAgentControlTaskEvent = Effect.fn("projectAgentControlTaskEvent")(function* (
  state: AgentControlTaskState | null,
  event: AgentControlTaskEvent,
): Effect.fn.Return<AgentControlTaskState, AgentControlProjectionCorruptError> {
  if (
    event.aggregateKind !== "task" ||
    event.aggregateId !== event.payload.taskId ||
    event.commandId !== event.correlationId ||
    event.causationEventId !== null ||
    event.authority !== "controller" ||
    event.streamVersion !== (state?.revision ?? 0) + 1 ||
    event.sequence <= (state?.sequence ?? 0)
  ) {
    return yield* corrupt();
  }

  switch (event.type) {
    case "agentControl.task.created":
      if (
        state !== null ||
        event.streamVersion !== 1 ||
        event.payload.createdAt !== event.occurredAt ||
        event.payload.source.projectId.length === 0 ||
        event.payload.sourceSnapshot.repositoryNodeId !== event.payload.source.repositoryNodeId ||
        event.payload.sourceSnapshot.issueNodeId !== event.payload.source.issueNodeId ||
        event.payload.sourceSnapshot.number !== event.payload.source.issueNumber ||
        event.payload.sourceSnapshot.url !== event.payload.source.issueUrl ||
        event.payload.sourceSnapshot.updatedAt !== event.payload.sourceUpdatedAt
      ) {
        return yield* corrupt();
      }
      return {
        schemaVersion: 1,
        taskId: event.payload.taskId,
        source: event.payload.source,
        status: event.payload.status,
        sourceGate: event.payload.sourceGate,
        stage: event.payload.stage,
        sourceUpdatedAt: event.payload.sourceUpdatedAt,
        githubIntakeSequence: event.payload.githubIntakeSequence,
        sourceSnapshot: event.payload.sourceSnapshot,
        createdAt: event.payload.createdAt,
        updatedAt: event.occurredAt,
        revision: event.streamVersion,
        sequence: event.sequence,
      };
    case "agentControl.task.sourceGate.changed":
      if (
        state === null ||
        event.payload.changedAt !== event.occurredAt ||
        event.payload.previousSourceGate !== state.sourceGate ||
        !sameSource(event.payload.source, state.source) ||
        event.payload.sourceSnapshot.repositoryNodeId !== state.source.repositoryNodeId ||
        event.payload.sourceSnapshot.issueNodeId !== state.source.issueNodeId ||
        event.payload.sourceSnapshot.number !== state.source.issueNumber ||
        event.payload.sourceSnapshot.url !== state.source.issueUrl ||
        event.payload.sourceSnapshot.updatedAt !== event.payload.sourceUpdatedAt ||
        event.payload.githubIntakeSequence < state.githubIntakeSequence
      ) {
        return yield* corrupt();
      }
      return {
        ...state,
        sourceGate: event.payload.sourceGate,
        sourceUpdatedAt: event.payload.sourceUpdatedAt,
        githubIntakeSequence: event.payload.githubIntakeSequence,
        sourceSnapshot: event.payload.sourceSnapshot,
        updatedAt: event.occurredAt,
        revision: event.streamVersion,
        sequence: event.sequence,
      };
    case "agentControl.task.needsAttentionMarked":
      if (
        state === null ||
        event.payload.markedAt !== event.occurredAt ||
        event.payload.previousStatus !== state.status ||
        event.payload.previousSourceGate !== state.sourceGate ||
        event.payload.githubIntakeSequence < state.githubIntakeSequence
      ) {
        return yield* corrupt();
      }
      return {
        ...state,
        status: "needs-attention",
        sourceGate: event.payload.sourceGate,
        sourceUpdatedAt: event.payload.sourceUpdatedAt,
        githubIntakeSequence: event.payload.githubIntakeSequence,
        updatedAt: event.occurredAt,
        revision: event.streamVersion,
        sequence: event.sequence,
      };
  }
});
