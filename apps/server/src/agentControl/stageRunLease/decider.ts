import {
  AgentControlStageRunLeaseRpcError,
  type AgentControlStageRunLeaseCommand,
  type AgentControlStageRunLeaseEventDraft,
  type AgentControlStageRunLeaseState,
  type EventId,
  type IsoDateTime,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

const error = (
  code: AgentControlStageRunLeaseRpcError["code"],
  command: AgentControlStageRunLeaseCommand,
) =>
  new AgentControlStageRunLeaseRpcError({
    code,
    operation: "dispatch",
    projectId: command.projectId,
    taskId: command.taskId,
  });

const sameScope = (
  state: AgentControlStageRunLeaseState,
  command: AgentControlStageRunLeaseCommand,
) =>
  state.leaseId === command.leaseId &&
  state.projectId === command.projectId &&
  state.taskId === command.taskId;

const requireOwnedReservation = Effect.fn("requireOwnedStageRunLeaseReservation")(function* (
  state: AgentControlStageRunLeaseState | null,
  command: Exclude<
    AgentControlStageRunLeaseCommand,
    { readonly type: "agentControl.stageRunLease.reserve" }
  >,
): Effect.fn.Return<AgentControlStageRunLeaseState, AgentControlStageRunLeaseRpcError> {
  if (state === null) return yield* error("lease-missing", command);
  if (!sameScope(state, command)) return yield* error("command-identity-mismatch", command);
  if (state.status !== "reserved") return yield* error("state-not-available", command);
  if (state.stageRunId !== command.stageRunId || state.attemptId !== command.attemptId) {
    return yield* error("command-identity-mismatch", command);
  }
  if (state.holderId !== command.holderId) return yield* error("holder-mismatch", command);
  if (state.fenceToken !== command.fenceToken) {
    return yield* error("fence-token-mismatch", command);
  }
  if (state.revision !== command.expectedRevision) {
    return yield* error("revision-conflict", command);
  }
  return state;
});

export const decideAgentControlStageRunLeaseCommand = Effect.fn(
  "decideAgentControlStageRunLeaseCommand",
)(function* (input: {
  readonly state: AgentControlStageRunLeaseState | null;
  readonly command: AgentControlStageRunLeaseCommand;
  readonly eventId: EventId;
  readonly occurredAt: IsoDateTime;
  readonly expiresAt: IsoDateTime | null;
}): Effect.fn.Return<
  ReadonlyArray<AgentControlStageRunLeaseEventDraft>,
  AgentControlStageRunLeaseRpcError
> {
  const { state, command, eventId, occurredAt, expiresAt } = input;
  if (command.type === "agentControl.stageRunLease.transition") {
    return yield* error("state-not-available", command);
  }
  if (command.type === "agentControl.stageRunLease.reserve") {
    if (state !== null && !sameScope(state, command)) {
      return yield* error("command-identity-mismatch", command);
    }
    if (state?.status === "reserved") return yield* error("lease-already-reserved", command);
    const expectedRevision = state?.revision ?? 0;
    if (command.expectedRevision !== expectedRevision) {
      return yield* error("revision-conflict", command);
    }
    const nextFenceToken = (state?.fenceToken ?? 0) + 1;
    if (command.fenceToken !== nextFenceToken) {
      return yield* error("fence-token-mismatch", command);
    }
    if (expiresAt === null) return yield* error("validation", command);
    return [
      {
        eventId,
        type: "agentControl.stageRunLease.reserved",
        aggregateKind: "stage-run-lease",
        aggregateId: command.leaseId,
        occurredAt,
        commandId: command.commandId,
        causationEventId: null,
        correlationId: command.commandId,
        authority: command.authority,
        metadata: { schemaVersion: 1 },
        payload: {
          leaseId: command.leaseId,
          projectId: command.projectId,
          taskId: command.taskId,
          stageRunId: command.stageRunId,
          attemptId: command.attemptId,
          taskRevision: command.taskRevision,
          githubIntakeSequence: command.githubIntakeSequence,
          sourceIdentityFingerprint: command.sourceIdentityFingerprint,
          holderId: command.holderId,
          fenceToken: command.fenceToken,
          acquiredAt: occurredAt,
          renewedAt: occurredAt,
          expiresAt,
        },
      },
    ];
  }

  yield* requireOwnedReservation(state, command);
  if (command.type === "agentControl.stageRunLease.renew") {
    if (expiresAt === null) return yield* error("validation", command);
    return [
      {
        eventId,
        type: "agentControl.stageRunLease.renewed",
        aggregateKind: "stage-run-lease",
        aggregateId: command.leaseId,
        occurredAt,
        commandId: command.commandId,
        causationEventId: null,
        correlationId: command.commandId,
        authority: command.authority,
        metadata: { schemaVersion: 1 },
        payload: {
          leaseId: command.leaseId,
          stageRunId: command.stageRunId,
          attemptId: command.attemptId,
          holderId: command.holderId,
          fenceToken: command.fenceToken,
          renewedAt: occurredAt,
          expiresAt,
        },
      },
    ];
  }
  return [
    {
      eventId,
      type: "agentControl.stageRunLease.releasedBeforeExecution",
      aggregateKind: "stage-run-lease",
      aggregateId: command.leaseId,
      occurredAt,
      commandId: command.commandId,
      causationEventId: null,
      correlationId: command.commandId,
      authority: command.authority,
      metadata: { schemaVersion: 1 },
      payload: {
        leaseId: command.leaseId,
        stageRunId: command.stageRunId,
        attemptId: command.attemptId,
        holderId: command.holderId,
        fenceToken: command.fenceToken,
        releasedAt: occurredAt,
      },
    },
  ];
});
