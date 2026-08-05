import {
  AgentControlProjectionCorruptError,
  type AgentControlStageRunLeaseEvent,
  type AgentControlStageRunLeaseState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR,
  canonicalTimestampMillis,
  validateAgentControlStageRunLeaseState,
} from "./invariant.ts";

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR,
  });

const validEnvelope = (
  state: AgentControlStageRunLeaseState | null,
  event: AgentControlStageRunLeaseEvent,
) => {
  const implementationRelease =
    event.type === "agentControl.stageRunLease.releasedAfterImplementation";
  return (
    event.aggregateKind === "stage-run-lease" &&
    event.aggregateId === event.payload.leaseId &&
    event.commandId === event.correlationId &&
    (implementationRelease ? event.causationEventId !== null : event.causationEventId === null) &&
    (event.authority === "controller" || event.authority === "system") &&
    event.streamVersion === (state?.revision ?? 0) + 1 &&
    event.sequence > (state?.sequence ?? 0) &&
    canonicalTimestampMillis(event.occurredAt) !== null
  );
};

export const projectAgentControlStageRunLeaseEvent = Effect.fn(
  "projectAgentControlStageRunLeaseEvent",
)(function* (
  state: AgentControlStageRunLeaseState | null,
  event: AgentControlStageRunLeaseEvent,
): Effect.fn.Return<AgentControlStageRunLeaseState, AgentControlProjectionCorruptError> {
  if (!validEnvelope(state, event)) return yield* corrupt();

  if (event.type === "agentControl.stageRunLease.reserved") {
    if (
      state?.status === "reserved" ||
      (state !== null &&
        (state.leaseId !== event.payload.leaseId ||
          state.projectId !== event.payload.projectId ||
          state.taskId !== event.payload.taskId ||
          event.payload.fenceToken !== state.fenceToken + 1)) ||
      (state === null && event.payload.fenceToken !== 1) ||
      event.occurredAt !== event.payload.acquiredAt ||
      event.occurredAt !== event.payload.renewedAt
    ) {
      return yield* corrupt();
    }
    return yield* validateAgentControlStageRunLeaseState({
      schemaVersion: 1,
      leaseId: event.payload.leaseId,
      projectId: event.payload.projectId,
      taskId: event.payload.taskId,
      stageRunId: event.payload.stageRunId,
      attemptId: event.payload.attemptId,
      taskRevision: event.payload.taskRevision,
      githubIntakeSequence: event.payload.githubIntakeSequence,
      sourceIdentityFingerprint: event.payload.sourceIdentityFingerprint,
      holderId: event.payload.holderId,
      fenceToken: event.payload.fenceToken,
      status: "reserved",
      acquiredAt: event.payload.acquiredAt,
      renewedAt: event.payload.renewedAt,
      expiresAt: event.payload.expiresAt,
      releasedAt: null,
      revision: event.streamVersion,
      sequence: event.sequence,
    });
  }

  if (
    state === null ||
    state.status !== "reserved" ||
    state.leaseId !== event.payload.leaseId ||
    state.stageRunId !== event.payload.stageRunId ||
    state.attemptId !== event.payload.attemptId ||
    state.holderId !== event.payload.holderId ||
    state.fenceToken !== event.payload.fenceToken
  ) {
    return yield* corrupt();
  }

  if (event.type === "agentControl.stageRunLease.renewed") {
    if (
      event.occurredAt !== event.payload.renewedAt ||
      canonicalTimestampMillis(event.payload.renewedAt)! <
        canonicalTimestampMillis(state.renewedAt)!
    ) {
      return yield* corrupt();
    }
    return yield* validateAgentControlStageRunLeaseState({
      ...state,
      renewedAt: event.payload.renewedAt,
      expiresAt: event.payload.expiresAt,
      revision: event.streamVersion,
      sequence: event.sequence,
    });
  }

  if (
    (event.type === "agentControl.stageRunLease.releasedAfterPlanning" ||
      event.type === "agentControl.stageRunLease.releasedAfterImplementation") &&
    (event.authority !== "system" ||
      event.payload.projectId !== state.projectId ||
      event.payload.taskId !== state.taskId ||
      event.payload.taskRevision !== state.taskRevision ||
      event.payload.githubIntakeSequence !== state.githubIntakeSequence ||
      event.payload.sourceIdentityFingerprint !== state.sourceIdentityFingerprint)
  ) {
    return yield* corrupt();
  }
  if (event.occurredAt !== event.payload.releasedAt) return yield* corrupt();
  return yield* validateAgentControlStageRunLeaseState({
    ...state,
    status: "released",
    releasedAt: event.payload.releasedAt,
    revision: event.streamVersion,
    sequence: event.sequence,
  });
});
