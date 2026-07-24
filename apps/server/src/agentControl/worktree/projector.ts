import {
  AgentControlProjectionCorruptError,
  type AgentControlWorktreeEvent,
  type AgentControlWorktreeReservationState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  AGENT_CONTROL_WORKTREE_PROJECTOR,
  validateAgentControlWorktreeReservationState,
} from "./invariant.ts";
import { canonicalTimestampMillis } from "../stageRunLease/invariant.ts";

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_WORKTREE_PROJECTOR,
  });

const validEnvelope = (
  state: AgentControlWorktreeReservationState | null,
  event: AgentControlWorktreeEvent,
) =>
  event.aggregateKind === "worktree-reservation" &&
  event.aggregateId === event.payload.reservationId &&
  event.commandId === event.correlationId &&
  event.causationEventId === null &&
  event.authority === "controller" &&
  event.streamVersion === (state?.revision ?? 0) + 1 &&
  event.sequence > (state?.sequence ?? 0) &&
  canonicalTimestampMillis(event.occurredAt) !== null;

export const projectAgentControlWorktreeEvent = Effect.fn("projectAgentControlWorktreeEvent")(
  function* (
    state: AgentControlWorktreeReservationState | null,
    event: AgentControlWorktreeEvent,
  ): Effect.fn.Return<AgentControlWorktreeReservationState, AgentControlProjectionCorruptError> {
    if (!validEnvelope(state, event)) return yield* corrupt();
    if (event.type === "agentControl.worktree.reserved") {
      if (state !== null || event.occurredAt !== event.payload.reservedAt) {
        return yield* corrupt();
      }
      return yield* validateAgentControlWorktreeReservationState({
        schemaVersion: 1,
        ...event.payload,
        status: "reserved",
        headCommitSha: null,
        attentionCode: null,
        createdAt: event.payload.reservedAt,
        updatedAt: event.payload.reservedAt,
        revision: event.streamVersion,
        sequence: event.sequence,
      });
    }
    if (
      state === null ||
      event.payload.projectId !== state.projectId ||
      event.payload.taskId !== state.taskId ||
      event.payload.stageRunId !== state.stageRunId ||
      event.payload.attemptId !== state.attemptId ||
      event.payload.leaseId !== state.leaseId ||
      event.payload.fenceToken !== state.fenceToken ||
      event.occurredAt !== event.payload.transitionedAt
    ) {
      return yield* corrupt();
    }
    if (event.type === "agentControl.worktree.materializationStarted") {
      if (state.status !== "reserved") return yield* corrupt();
      return yield* validateAgentControlWorktreeReservationState({
        ...state,
        status: "materializing",
        updatedAt: event.occurredAt,
        revision: event.streamVersion,
        sequence: event.sequence,
      });
    }
    if (event.type === "agentControl.worktree.ready") {
      if (state.status !== "materializing" || event.payload.headCommitSha !== state.baseCommitSha) {
        return yield* corrupt();
      }
      return yield* validateAgentControlWorktreeReservationState({
        ...state,
        status: "ready",
        headCommitSha: event.payload.headCommitSha,
        updatedAt: event.occurredAt,
        revision: event.streamVersion,
        sequence: event.sequence,
      });
    }
    if (state.status !== "reserved" && state.status !== "materializing") {
      return yield* corrupt();
    }
    return yield* validateAgentControlWorktreeReservationState({
      ...state,
      status: "needs-attention",
      attentionCode: event.payload.attentionCode,
      updatedAt: event.occurredAt,
      revision: event.streamVersion,
      sequence: event.sequence,
    });
  },
);
