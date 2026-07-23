import {
  AgentControlTaskRpcError,
  type AgentControlTaskCommand,
  type AgentControlTaskEventDraft,
  type AgentControlTaskState,
  type EventId,
  type IsoDateTime,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

const error = (code: AgentControlTaskRpcError["code"], command: AgentControlTaskCommand) =>
  new AgentControlTaskRpcError({
    code,
    operation: "dispatch",
    projectId: command.projectId,
    taskId: command.taskId,
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

const sameSnapshot = (
  left: AgentControlTaskState["sourceSnapshot"],
  right: AgentControlTaskState["sourceSnapshot"],
) =>
  left.repositoryNodeId === right.repositoryNodeId &&
  left.issueNodeId === right.issueNodeId &&
  left.number === right.number &&
  left.url === right.url &&
  left.state === right.state &&
  left.title === right.title &&
  left.body === right.body &&
  left.contentTrust === right.contentTrust &&
  left.updatedAt === right.updatedAt &&
  left.timelineComplete === right.timelineComplete &&
  left.ready === right.ready &&
  left.paused === right.paused &&
  left.eligible === right.eligible &&
  left.eligibilityReason === right.eligibilityReason;

const normalizedGate = (
  snapshot: AgentControlTaskState["sourceSnapshot"],
): AgentControlTaskState["sourceGate"] => {
  if (!snapshot.timelineComplete || snapshot.eligibilityReason === "timeline-invalid") {
    return "timeline-invalid";
  }
  if (snapshot.state === "closed" || snapshot.eligibilityReason === "closed") return "closed";
  if (snapshot.paused || snapshot.eligibilityReason === "paused") return "paused";
  return snapshot.eligible && snapshot.ready ? "eligible" : "not-ready";
};

export const decideAgentControlTaskCommand = Effect.fn("decideAgentControlTaskCommand")(
  function* (input: {
    readonly state: AgentControlTaskState | null;
    readonly command: AgentControlTaskCommand;
    readonly eventId: EventId;
    readonly occurredAt: IsoDateTime;
  }): Effect.fn.Return<ReadonlyArray<AgentControlTaskEventDraft>, AgentControlTaskRpcError> {
    const { state, command, eventId, occurredAt } = input;
    if (command.type === "agentControl.task.status.set") {
      return yield* error("state-not-available", command);
    }

    if (command.type === "agentControl.task.createFromGithubIssue") {
      if (
        state !== null ||
        command.expectedRevision !== 0 ||
        command.source.projectId !== command.projectId ||
        command.sourceSnapshot.repositoryNodeId !== command.source.repositoryNodeId ||
        command.sourceSnapshot.issueNodeId !== command.source.issueNodeId ||
        command.sourceSnapshot.number !== command.source.issueNumber ||
        command.sourceSnapshot.url !== command.source.issueUrl ||
        command.sourceSnapshot.updatedAt !== command.sourceUpdatedAt ||
        command.sourceGate !== "eligible" ||
        normalizedGate(command.sourceSnapshot) !== command.sourceGate
      ) {
        return yield* error(
          state === null ? "source-identity-conflict" : "revision-conflict",
          command,
        );
      }
      return [
        {
          eventId,
          type: "agentControl.task.created",
          aggregateKind: "task",
          aggregateId: command.taskId,
          occurredAt,
          commandId: command.commandId,
          causationEventId: null,
          correlationId: command.commandId,
          authority: "controller",
          metadata: { schemaVersion: 1 },
          payload: {
            taskId: command.taskId,
            source: command.source,
            status: "candidate",
            sourceGate: command.sourceGate,
            stage: "intake",
            sourceUpdatedAt: command.sourceUpdatedAt,
            githubIntakeSequence: command.githubIntakeSequence,
            sourceSnapshot: command.sourceSnapshot,
            createdAt: occurredAt,
          },
        },
      ];
    }

    if (state === null) return yield* error("task-missing", command);
    if (state.revision !== command.expectedRevision) {
      return yield* error("revision-conflict", command);
    }
    if (state.source.projectId !== command.projectId) {
      return yield* error("source-identity-conflict", command);
    }

    if (command.type === "agentControl.task.sourceGate.refresh") {
      if (
        !sameSource(state.source, command.source) ||
        command.sourceSnapshot.repositoryNodeId !== state.source.repositoryNodeId ||
        command.sourceSnapshot.issueNodeId !== state.source.issueNodeId ||
        command.sourceSnapshot.number !== state.source.issueNumber ||
        command.sourceSnapshot.url !== state.source.issueUrl ||
        command.sourceSnapshot.updatedAt !== command.sourceUpdatedAt ||
        (command.sourceGate !== "identity-invalid" &&
          command.sourceGate !== "source-missing" &&
          normalizedGate(command.sourceSnapshot) !== command.sourceGate)
      ) {
        return yield* error("source-identity-conflict", command);
      }
      if (
        state.sourceGate === command.sourceGate &&
        state.sourceUpdatedAt === command.sourceUpdatedAt &&
        state.githubIntakeSequence === command.githubIntakeSequence &&
        sameSnapshot(state.sourceSnapshot, command.sourceSnapshot)
      ) {
        return [];
      }
      return [
        {
          eventId,
          type: "agentControl.task.sourceGate.changed",
          aggregateKind: "task",
          aggregateId: command.taskId,
          occurredAt,
          commandId: command.commandId,
          causationEventId: null,
          correlationId: command.commandId,
          authority: "controller",
          metadata: { schemaVersion: 1 },
          payload: {
            taskId: command.taskId,
            source: state.source,
            previousSourceGate: state.sourceGate,
            sourceGate: command.sourceGate,
            sourceUpdatedAt: command.sourceUpdatedAt,
            githubIntakeSequence: command.githubIntakeSequence,
            sourceSnapshot: command.sourceSnapshot,
            changedAt: occurredAt,
          },
        },
      ];
    }

    if (
      state.status === "needs-attention" &&
      state.sourceGate === command.sourceGate &&
      state.githubIntakeSequence === command.githubIntakeSequence
    ) {
      return [];
    }
    return [
      {
        eventId,
        type: "agentControl.task.needsAttentionMarked",
        aggregateKind: "task",
        aggregateId: command.taskId,
        occurredAt,
        commandId: command.commandId,
        causationEventId: null,
        correlationId: command.commandId,
        authority: "controller",
        metadata: { schemaVersion: 1 },
        payload: {
          taskId: command.taskId,
          previousStatus: state.status,
          previousSourceGate: state.sourceGate,
          sourceGate: command.sourceGate,
          sourceUpdatedAt: command.sourceUpdatedAt,
          githubIntakeSequence: command.githubIntakeSequence,
          markedAt: occurredAt,
        },
      },
    ];
  },
);
