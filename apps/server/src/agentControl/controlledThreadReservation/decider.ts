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

const sameStableBinding = (
  state: AgentControlControlledThreadReservationState,
  command: AgentControlControlledThreadReservationCommand,
) =>
  state.controlledThreadReservationId === command.controlledThreadReservationId &&
  state.threadId === command.threadId &&
  state.projectId === command.projectId &&
  state.taskId === command.taskId &&
  state.taskRevision === command.taskRevision &&
  state.githubIntakeSequence === command.githubIntakeSequence &&
  state.sourceIdentityFingerprint === command.sourceIdentityFingerprint &&
  state.stageRunId === command.stageRunId &&
  state.attemptId === command.attemptId &&
  state.roleId === command.roleId &&
  state.stageKind === command.stageKind &&
  state.stageOrdinal === command.stageOrdinal &&
  state.attemptOrdinal === command.attemptOrdinal &&
  state.leaseId === command.leaseId &&
  state.fenceToken === command.fenceToken &&
  state.worktreeReservationId === command.worktreeReservationId;

const sameMaterializingBinding = (
  state: Extract<AgentControlControlledThreadReservationState, { status: "materializing" }>,
  command: Extract<
    AgentControlControlledThreadReservationCommand,
    { type: "agentControl.controlledThreadReservation.bindMaterialization" }
  >,
) =>
  state.coordinatorCommandId === command.coordinatorCommandId &&
  state.coordinatorCommandFingerprint === command.coordinatorCommandFingerprint &&
  state.materializingTransitionCommandId === command.materializingTransitionCommandId &&
  state.materializationCommandId === command.materializationCommandId &&
  state.materializationCommandFingerprint === command.materializationCommandFingerprint &&
  state.leaseHolderId === command.leaseHolderId &&
  state.materializingAt === command.materializingAt;

const stablePayload = (state: AgentControlControlledThreadReservationState) => ({
  controlledThreadReservationId: state.controlledThreadReservationId,
  threadId: state.threadId,
  projectId: state.projectId,
  taskId: state.taskId,
  taskRevision: state.taskRevision,
  githubIntakeSequence: state.githubIntakeSequence,
  sourceIdentityFingerprint: state.sourceIdentityFingerprint,
  stageRunId: state.stageRunId,
  attemptId: state.attemptId,
  roleId: state.roleId,
  stageKind: state.stageKind,
  stageOrdinal: state.stageOrdinal,
  attemptOrdinal: state.attemptOrdinal,
  leaseId: state.leaseId,
  fenceToken: state.fenceToken,
  worktreeReservationId: state.worktreeReservationId,
  preparedAt: state.preparedAt,
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
  const isPlanningIdentity =
    command.stageKind === "planning" &&
    command.roleId === "planning" &&
    command.stageOrdinal === 1 &&
    command.attemptOrdinal === 1;
  const isImplementationIdentity =
    command.stageKind === "implementation" &&
    command.roleId === "implementer" &&
    command.stageOrdinal === 2 &&
    command.attemptOrdinal === 1;
  const isVerificationIdentity =
    command.stageKind === "verification" &&
    command.roleId === "verifier" &&
    command.stageOrdinal === 3 &&
    command.attemptOrdinal === 1;
  if (!isPlanningIdentity && !isImplementationIdentity && !isVerificationIdentity) {
    return yield* error("controlled-thread-reservation-identity-conflict", command);
  }
  if (command.type === "agentControl.controlledThreadReservation.prepare") {
    if (state !== null) {
      yield* validateAgentControlControlledThreadReservationState(state).pipe(
        Effect.mapError(() => error("controlled-thread-reservation-corrupt", command)),
      );
      return yield* error("controlled-thread-reservation-identity-conflict", command);
    }
    if (command.expectedRevision !== 0) return yield* error("revision-conflict", command);
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
    }).pipe(
      Effect.mapError(() => error("controlled-thread-reservation-identity-conflict", command)),
    );
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
  }

  if (state === null) return yield* error("controlled-thread-reservation-missing", command);
  yield* validateAgentControlControlledThreadReservationState(state).pipe(
    Effect.mapError(() => error("controlled-thread-reservation-corrupt", command)),
  );
  if (!sameStableBinding(state, command)) {
    return yield* error("controlled-thread-reservation-identity-conflict", command);
  }
  if (command.type === "agentControl.controlledThreadReservation.beginMaterialization") {
    if (state.status !== "prepared") return yield* error("state-not-available", command);
    if (
      command.expectedRevision !== 1 ||
      command.commandId !== command.materializingTransitionCommandId
    ) {
      return yield* error("revision-conflict", command);
    }
    if (occurredAt !== command.materializingAt) {
      return yield* error("controlled-thread-reservation-identity-conflict", command);
    }
    return [
      {
        eventId,
        type: "agentControl.controlledThreadReservation.materializing",
        aggregateKind: "controlled-thread-reservation",
        aggregateId: command.controlledThreadReservationId,
        occurredAt,
        commandId: command.commandId,
        causationEventId: null,
        correlationId: command.coordinatorCommandId,
        authority: "controller",
        metadata: { schemaVersion: 1 },
        payload: {
          ...stablePayload(state),
          status: "materializing",
          coordinatorCommandId: command.coordinatorCommandId,
          coordinatorCommandFingerprint: command.coordinatorCommandFingerprint,
          materializingTransitionCommandId: command.materializingTransitionCommandId,
          materializationCommandId: command.materializationCommandId,
          materializationCommandFingerprint: command.materializationCommandFingerprint,
          leaseHolderId: command.leaseHolderId,
          materializingAt: command.materializingAt,
        },
      },
    ];
  }

  if (state.status !== "materializing") return yield* error("state-not-available", command);
  if (
    command.expectedRevision !== 2 ||
    command.commandId !== command.boundTransitionCommandId ||
    !sameMaterializingBinding(state, command) ||
    occurredAt !== command.boundAt
  ) {
    return yield* error("controlled-thread-reservation-identity-conflict", command);
  }
  return [
    {
      eventId,
      type: "agentControl.controlledThreadReservation.bound",
      aggregateKind: "controlled-thread-reservation",
      aggregateId: command.controlledThreadReservationId,
      occurredAt,
      commandId: command.commandId,
      causationEventId: null,
      correlationId: command.coordinatorCommandId,
      authority: "controller",
      metadata: { schemaVersion: 1 },
      payload: {
        ...stablePayload(state),
        status: "bound",
        coordinatorCommandId: state.coordinatorCommandId,
        coordinatorCommandFingerprint: state.coordinatorCommandFingerprint,
        materializingTransitionCommandId: state.materializingTransitionCommandId,
        materializationCommandId: state.materializationCommandId,
        materializationCommandFingerprint: state.materializationCommandFingerprint,
        leaseHolderId: state.leaseHolderId,
        materializingAt: state.materializingAt,
        boundTransitionCommandId: command.boundTransitionCommandId,
        orchestrationResultSequence: command.orchestrationResultSequence,
        materializedAt: command.materializedAt,
        boundAt: command.boundAt,
      },
    },
  ];
});
