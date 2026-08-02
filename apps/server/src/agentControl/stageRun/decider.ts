import {
  AgentControlStageRunRpcError,
  type AgentControlStageRunCommand,
  type AgentControlStageRunEventDraft,
  type AgentControlStageRunState,
  type EventId,
  type IsoDateTime,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

const error = (code: AgentControlStageRunRpcError["code"], command: AgentControlStageRunCommand) =>
  new AgentControlStageRunRpcError({
    code,
    operation: "dispatch",
    projectId: command.projectId,
    taskId: command.taskId,
  });

const samePreparedSnapshot = (
  state: AgentControlStageRunState,
  command: AgentControlStageRunCommand,
) =>
  state.projectId === command.projectId &&
  state.taskId === command.taskId &&
  state.stageRunId === command.stageRunId &&
  state.attemptId === command.attemptId &&
  state.roleId === command.roleId &&
  state.stageKind === command.stageKind &&
  state.stageOrdinal === command.stageOrdinal &&
  state.attemptOrdinal === command.attemptOrdinal &&
  state.status === "prepared" &&
  state.taskRevision === command.taskRevision &&
  state.githubIntakeSequence === command.githubIntakeSequence &&
  state.sourceIdentityFingerprint === command.sourceIdentityFingerprint;

export const decideAgentControlStageRunCommand = Effect.fn("decideAgentControlStageRunCommand")(
  function* (input: {
    readonly state: AgentControlStageRunState | null;
    readonly command: AgentControlStageRunCommand;
    readonly eventId: EventId;
    readonly occurredAt: IsoDateTime;
  }): Effect.fn.Return<
    ReadonlyArray<
      Extract<AgentControlStageRunEventDraft, { type: "agentControl.stageRun.prepared" }>
    >,
    AgentControlStageRunRpcError
  > {
    const { state, command, eventId, occurredAt } = input;
    if (command.type === "agentControl.stageRun.status.set") {
      return yield* error("state-not-available", command);
    }
    if (state !== null) {
      return samePreparedSnapshot(state, command)
        ? []
        : yield* error("stage-run-identity-conflict", command);
    }
    if (command.expectedRevision !== 0) {
      return yield* error("revision-conflict", command);
    }
    if (
      command.stageKind !== "planning" ||
      command.stageOrdinal !== 1 ||
      command.attemptOrdinal !== 1 ||
      command.roleId !== "planning"
    ) {
      return yield* error("state-not-available", command);
    }
    return [
      {
        eventId,
        type: "agentControl.stageRun.prepared",
        aggregateKind: "stage-run",
        aggregateId: command.stageRunId,
        occurredAt,
        commandId: command.commandId,
        causationEventId: null,
        correlationId: command.commandId,
        authority: "controller",
        metadata: { schemaVersion: 1 },
        payload: {
          projectId: command.projectId,
          taskId: command.taskId,
          stageRunId: command.stageRunId,
          attemptId: command.attemptId,
          roleId: command.roleId,
          stageKind: command.stageKind,
          stageOrdinal: command.stageOrdinal,
          attemptOrdinal: command.attemptOrdinal,
          status: "prepared",
          taskRevision: command.taskRevision,
          githubIntakeSequence: command.githubIntakeSequence,
          sourceIdentityFingerprint: command.sourceIdentityFingerprint,
          preparedAt: occurredAt,
        },
      },
    ];
  },
);
