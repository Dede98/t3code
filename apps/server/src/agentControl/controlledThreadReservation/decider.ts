import {
  AgentControlControlledThreadReservationRpcError,
  type AgentControlControlledThreadReservationCommand,
  type AgentControlControlledThreadReservationEventDraft,
  type AgentControlControlledThreadReservationState,
  type EventId,
  type IsoDateTime,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { validateAgentControlControlledThreadReservationState } from "./invariant.ts";

const error = (
  code: AgentControlControlledThreadReservationRpcError["code"],
  command: AgentControlControlledThreadReservationCommand,
) =>
  new AgentControlControlledThreadReservationRpcError({
    code,
    operation: "dispatch",
    projectId: command.projectId,
    taskId: command.taskId,
    controlledThreadReservationId: command.controlledThreadReservationId,
  });

export const decideAgentControlControlledThreadReservationCommand = Effect.fn(
  "decideAgentControlControlledThreadReservationCommand",
)(function* (input: {
  readonly state: AgentControlControlledThreadReservationState | null;
  readonly command: AgentControlControlledThreadReservationCommand;
  readonly eventId: EventId;
  readonly occurredAt: IsoDateTime;
}): Effect.fn.Return<
  ReadonlyArray<AgentControlControlledThreadReservationEventDraft>,
  AgentControlControlledThreadReservationRpcError
> {
  const { state, command, eventId, occurredAt } = input;
  if (command.authority !== "controller") {
    return yield* error("controlled-thread-reservation-identity-conflict", command);
  }
  if (command.type === "agentControl.controlledThreadReservation.transition") {
    return yield* error("state-not-available", command);
  }
  if (state !== null) {
    yield* validateAgentControlControlledThreadReservationState(state).pipe(
      Effect.mapError(() => error("controlled-thread-reservation-corrupt", command)),
    );
    return yield* error("controlled-thread-reservation-identity-conflict", command);
  }
  if (command.expectedRevision !== 0) return yield* error("revision-conflict", command);
  if (
    command.stageKind !== "planning" ||
    command.stageOrdinal !== 1 ||
    command.attemptOrdinal !== 1
  ) {
    return yield* error("controlled-thread-reservation-identity-conflict", command);
  }

  yield* validateAgentControlControlledThreadReservationState({
    schemaVersion: 1,
    controlledThreadReservationId: command.controlledThreadReservationId,
    threadId: command.threadId,
    projectId: command.projectId,
    taskId: command.taskId,
    taskRevision: command.taskRevision,
    githubIntakeSequence: command.githubIntakeSequence,
    sourceIdentityFingerprint: command.sourceIdentityFingerprint,
    stageRunId: command.stageRunId,
    attemptId: command.attemptId,
    roleId: command.roleId,
    stageKind: command.stageKind,
    stageOrdinal: command.stageOrdinal,
    attemptOrdinal: command.attemptOrdinal,
    leaseId: command.leaseId,
    fenceToken: command.fenceToken,
    worktreeReservationId: command.worktreeReservationId,
    status: "prepared",
    revision: 1,
    sequence: 1,
    preparedAt: occurredAt,
  }).pipe(Effect.mapError(() => error("controlled-thread-reservation-identity-conflict", command)));

  return [
    {
      eventId,
      type: "agentControl.controlledThreadReservation.prepared",
      aggregateKind: "controlled-thread-reservation",
      aggregateId: command.controlledThreadReservationId,
      occurredAt,
      commandId: command.commandId,
      causationEventId: null,
      correlationId: command.commandId,
      authority: "controller",
      metadata: { schemaVersion: 1 },
      payload: {
        controlledThreadReservationId: command.controlledThreadReservationId,
        threadId: command.threadId,
        projectId: command.projectId,
        taskId: command.taskId,
        taskRevision: command.taskRevision,
        githubIntakeSequence: command.githubIntakeSequence,
        sourceIdentityFingerprint: command.sourceIdentityFingerprint,
        stageRunId: command.stageRunId,
        attemptId: command.attemptId,
        roleId: command.roleId,
        stageKind: command.stageKind,
        stageOrdinal: command.stageOrdinal,
        attemptOrdinal: command.attemptOrdinal,
        leaseId: command.leaseId,
        fenceToken: command.fenceToken,
        worktreeReservationId: command.worktreeReservationId,
        status: "prepared",
        preparedAt: occurredAt,
      },
    },
  ];
});
