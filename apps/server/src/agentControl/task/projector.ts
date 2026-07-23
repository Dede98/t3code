import {
  AgentControlProjectionCorruptError,
  type AgentControlTaskEvent,
  type AgentControlTaskSourceSnapshot,
  type AgentControlTaskState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { normalizedTaskSourceGate, sameTaskSource } from "./decider.ts";
import { compareAgentControlTaskSourceTimestamps } from "./sourceTimestamp.ts";

export const AGENT_CONTROL_TASK_PROJECTOR = "agent-control-task-v1";

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_TASK_PROJECTOR,
  });

const sourceIdentityMatchesSnapshot = (
  source: AgentControlTaskState["source"],
  snapshot: AgentControlTaskSourceSnapshot,
  sourceUpdatedAt: string,
) =>
  snapshot.repositoryNodeId === source.repositoryNodeId &&
  snapshot.issueNodeId === source.issueNodeId &&
  snapshot.number === source.issueNumber &&
  snapshot.url === source.issueUrl &&
  compareAgentControlTaskSourceTimestamps(snapshot.updatedAt, sourceUpdatedAt) === 0;

const monotoneSource = (
  state: AgentControlTaskState,
  input: {
    readonly githubIntakeSequence: number;
    readonly sourceUpdatedAt: string;
  },
) => {
  const timestampOrder = compareAgentControlTaskSourceTimestamps(
    input.sourceUpdatedAt,
    state.sourceUpdatedAt,
  );
  return (
    input.githubIntakeSequence > state.githubIntakeSequence &&
    timestampOrder !== null &&
    timestampOrder >= 0
  );
};

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
  if (state?.sourceGate === "identity-invalid") return yield* corrupt();

  switch (event.type) {
    case "agentControl.task.created":
      if (
        state !== null ||
        event.streamVersion !== 1 ||
        event.payload.createdAt !== event.occurredAt ||
        event.payload.source.projectId.length === 0 ||
        event.payload.sourceGate !== "eligible" ||
        normalizedTaskSourceGate(event.payload.sourceSnapshot) !== "eligible" ||
        !sourceIdentityMatchesSnapshot(
          event.payload.source,
          event.payload.sourceSnapshot,
          event.payload.sourceUpdatedAt,
        )
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
        !sameTaskSource(event.payload.source, state.source) ||
        !sourceIdentityMatchesSnapshot(
          state.source,
          event.payload.sourceSnapshot,
          event.payload.sourceUpdatedAt,
        ) ||
        !monotoneSource(state, event.payload) ||
        (event.payload.sourceGate !== "identity-invalid" &&
          event.payload.sourceGate !== "source-missing" &&
          normalizedTaskSourceGate(event.payload.sourceSnapshot) !== event.payload.sourceGate) ||
        (state.status === "needs-attention" &&
          state.sourceGate === "source-missing" &&
          event.payload.sourceGate === "eligible")
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
        !sameTaskSource(event.payload.source, state.source) ||
        !sourceIdentityMatchesSnapshot(
          state.source,
          event.payload.sourceSnapshot,
          event.payload.sourceUpdatedAt,
        ) ||
        !monotoneSource(state, event.payload)
      ) {
        return yield* corrupt();
      }
      return {
        ...state,
        status: "needs-attention",
        sourceGate: event.payload.sourceGate,
        sourceUpdatedAt: event.payload.sourceUpdatedAt,
        githubIntakeSequence: event.payload.githubIntakeSequence,
        sourceSnapshot: event.payload.sourceSnapshot,
        updatedAt: event.occurredAt,
        revision: event.streamVersion,
        sequence: event.sequence,
      };
    case "agentControl.task.sourceMissingRecovered":
      if (
        state === null ||
        event.payload.recoveredAt !== event.occurredAt ||
        state.status !== "needs-attention" ||
        state.sourceGate !== "source-missing" ||
        event.payload.previousStatus !== state.status ||
        event.payload.previousSourceGate !== state.sourceGate ||
        event.payload.status !== "candidate" ||
        event.payload.sourceGate !== "eligible" ||
        !sameTaskSource(event.payload.source, state.source) ||
        !sourceIdentityMatchesSnapshot(
          state.source,
          event.payload.sourceSnapshot,
          event.payload.sourceUpdatedAt,
        ) ||
        normalizedTaskSourceGate(event.payload.sourceSnapshot) !== "eligible" ||
        !monotoneSource(state, event.payload)
      ) {
        return yield* corrupt();
      }
      return {
        ...state,
        status: "candidate",
        sourceGate: "eligible",
        sourceUpdatedAt: event.payload.sourceUpdatedAt,
        githubIntakeSequence: event.payload.githubIntakeSequence,
        sourceSnapshot: event.payload.sourceSnapshot,
        updatedAt: event.occurredAt,
        revision: event.streamVersion,
        sequence: event.sequence,
      };
  }
});
