import {
  AgentControlWorktreeRpcError,
  type AgentControlWorktreeCommand,
  type AgentControlWorktreeEventDraft,
  type AgentControlWorktreeReservationState,
  type EventId,
  type IsoDateTime,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

const error = (code: AgentControlWorktreeRpcError["code"], command: AgentControlWorktreeCommand) =>
  new AgentControlWorktreeRpcError({
    code,
    operation: command.type === "agentControl.worktree.reserve" ? "reserve" : "materialize",
    projectId: command.projectId,
    taskId: command.taskId,
    reservationId: command.reservationId,
  });

const sameBinding = (
  state: AgentControlWorktreeReservationState,
  command: AgentControlWorktreeCommand,
) =>
  state.reservationId === command.reservationId &&
  state.projectId === command.projectId &&
  state.taskId === command.taskId &&
  state.taskRevision === command.taskRevision &&
  state.githubIntakeSequence === command.githubIntakeSequence &&
  state.sourceIdentityFingerprint === command.sourceIdentityFingerprint &&
  state.stageRunId === command.stageRunId &&
  state.attemptId === command.attemptId &&
  state.leaseId === command.leaseId &&
  state.fenceToken === command.fenceToken;

const transitionPayload = (command: AgentControlWorktreeCommand, transitionedAt: IsoDateTime) => ({
  reservationId: command.reservationId,
  projectId: command.projectId,
  taskId: command.taskId,
  stageRunId: command.stageRunId,
  attemptId: command.attemptId,
  leaseId: command.leaseId,
  fenceToken: command.fenceToken,
  transitionedAt,
});

export const decideAgentControlWorktreeCommand = Effect.fn("decideAgentControlWorktreeCommand")(
  function* (input: {
    readonly state: AgentControlWorktreeReservationState | null;
    readonly command: AgentControlWorktreeCommand;
    readonly eventId: EventId;
    readonly occurredAt: IsoDateTime;
  }): Effect.fn.Return<
    ReadonlyArray<AgentControlWorktreeEventDraft>,
    AgentControlWorktreeRpcError
  > {
    const { state, command, eventId, occurredAt } = input;
    if (command.type === "agentControl.worktree.reserve") {
      if (state !== null) return yield* error("reservation-conflict", command);
      if (command.expectedRevision !== 0) return yield* error("revision-conflict", command);
      return [
        {
          eventId,
          type: "agentControl.worktree.reserved",
          aggregateKind: "worktree-reservation",
          aggregateId: command.reservationId,
          occurredAt,
          commandId: command.commandId,
          causationEventId: null,
          correlationId: command.commandId,
          authority: "controller",
          metadata: { schemaVersion: 1 },
          payload: {
            reservationId: command.reservationId,
            projectId: command.projectId,
            taskId: command.taskId,
            taskRevision: command.taskRevision,
            githubIntakeSequence: command.githubIntakeSequence,
            sourceIdentityFingerprint: command.sourceIdentityFingerprint,
            stageRunId: command.stageRunId,
            attemptId: command.attemptId,
            leaseId: command.leaseId,
            fenceToken: command.fenceToken,
            repository: command.repository,
            repositoryWorkspace: command.repositoryWorkspace,
            repositoryCommonDir: command.repositoryCommonDir,
            baseRef: command.baseRef,
            baseCommitSha: command.baseCommitSha,
            branchName: command.branchName,
            internalWorktreePath: command.internalWorktreePath,
            worktreeRootDevice: command.worktreeRootDevice,
            worktreeRootInode: command.worktreeRootInode,
            worktreeParentDevice: command.worktreeParentDevice,
            worktreeParentInode: command.worktreeParentInode,
            reservedAt: occurredAt,
          },
        },
      ];
    }
    if (state === null) return yield* error("reservation-missing", command);
    if (!sameBinding(state, command)) return yield* error("command-identity-mismatch", command);
    if (state.revision !== command.expectedRevision) {
      return yield* error("revision-conflict", command);
    }

    const envelope = {
      eventId,
      aggregateKind: "worktree-reservation" as const,
      aggregateId: command.reservationId,
      occurredAt,
      commandId: command.commandId,
      causationEventId: null,
      correlationId: command.commandId,
      authority: "controller" as const,
      metadata: { schemaVersion: 1 as const },
    };
    if (command.type === "agentControl.worktree.materialization.start") {
      if (state.status === "materializing" || state.status === "ready") return [];
      if (state.status !== "reserved") return yield* error("state-not-available", command);
      return [
        {
          ...envelope,
          type: "agentControl.worktree.materializationStarted",
          payload: transitionPayload(command, occurredAt),
        },
      ];
    }
    if (command.type === "agentControl.worktree.ready") {
      if (state.status === "ready") {
        if (
          state.headCommitSha === command.headCommitSha &&
          state.ownershipFingerprint === command.ownershipFingerprint &&
          state.verifiedAt === command.verifiedAt
        ) {
          return [];
        }
        return yield* error("command-identity-mismatch", command);
      }
      if (state.status !== "materializing") return yield* error("state-not-available", command);
      return [
        {
          ...envelope,
          type: "agentControl.worktree.ready",
          payload: {
            ...transitionPayload(command, occurredAt),
            headCommitSha: command.headCommitSha,
            ownershipFingerprint: command.ownershipFingerprint,
            verifiedAt: command.verifiedAt,
          },
        },
      ];
    }
    if (state.status === "needs-attention" && state.attentionCode === command.attentionCode) {
      return [];
    }
    if (state.status !== "reserved" && state.status !== "materializing") {
      return yield* error("state-not-available", command);
    }
    return [
      {
        ...envelope,
        type: "agentControl.worktree.needsAttention",
        payload: {
          ...transitionPayload(command, occurredAt),
          attentionCode: command.attentionCode,
        },
      },
    ];
  },
);
