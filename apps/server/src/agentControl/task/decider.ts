import {
  AgentControlTaskRpcError,
  type AgentControlTaskCommand,
  type AgentControlTaskEventDraft,
  type AgentControlTaskSourceSnapshot,
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

export const sameTaskSource = (
  left: AgentControlTaskState["source"],
  right: AgentControlTaskState["source"],
) =>
  left.projectId === right.projectId &&
  left.repositoryNodeId === right.repositoryNodeId &&
  left.issueNodeId === right.issueNodeId &&
  left.issueNumber === right.issueNumber &&
  left.issueUrl === right.issueUrl;

export const sameTaskSourceSnapshot = (
  left: AgentControlTaskSourceSnapshot,
  right: AgentControlTaskSourceSnapshot,
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

export const normalizedTaskSourceGate = (
  snapshot: AgentControlTaskSourceSnapshot,
): AgentControlTaskState["sourceGate"] => {
  if (!snapshot.timelineComplete || snapshot.eligibilityReason === "timeline-invalid") {
    return "timeline-invalid";
  }
  if (snapshot.state === "closed" || snapshot.eligibilityReason === "closed") {
    return "closed";
  }
  if (snapshot.paused || snapshot.eligibilityReason === "paused") return "paused";
  return snapshot.eligible && snapshot.ready ? "eligible" : "not-ready";
};

const sourceIdentityMatchesSnapshot = (
  source: AgentControlTaskState["source"],
  snapshot: AgentControlTaskSourceSnapshot,
  sourceUpdatedAt: IsoDateTime,
) =>
  snapshot.repositoryNodeId === source.repositoryNodeId &&
  snapshot.issueNodeId === source.issueNodeId &&
  snapshot.number === source.issueNumber &&
  snapshot.url === source.issueUrl &&
  snapshot.updatedAt === sourceUpdatedAt;

const sameSourceMutation = (
  state: AgentControlTaskState,
  command: Extract<
    AgentControlTaskCommand,
    {
      readonly type:
        | "agentControl.task.sourceGate.refresh"
        | "agentControl.task.markNeedsAttention"
        | "agentControl.task.recoverSourceMissing";
    }
  >,
) =>
  sameTaskSource(state.source, command.source) &&
  state.sourceGate === command.sourceGate &&
  state.sourceUpdatedAt === command.sourceUpdatedAt &&
  sameTaskSourceSnapshot(state.sourceSnapshot, command.sourceSnapshot);

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

    if (
      command.sourcePrecondition.projectId !== command.projectId ||
      command.sourcePrecondition.githubIntakeSequence !== command.githubIntakeSequence ||
      command.sourcePrecondition.repositoryNodeId !== command.source.repositoryNodeId
    ) {
      return yield* error("source-state-conflict", command);
    }

    if (command.type === "agentControl.task.createFromGithubIssue") {
      if (
        state !== null ||
        command.expectedRevision !== 0 ||
        command.source.projectId !== command.projectId ||
        !sourceIdentityMatchesSnapshot(
          command.source,
          command.sourceSnapshot,
          command.sourceUpdatedAt,
        ) ||
        command.sourceGate !== "eligible" ||
        normalizedTaskSourceGate(command.sourceSnapshot) !== command.sourceGate
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
    if (
      state.source.projectId !== command.projectId ||
      !sameTaskSource(state.source, command.source) ||
      !sourceIdentityMatchesSnapshot(
        command.source,
        command.sourceSnapshot,
        command.sourceUpdatedAt,
      )
    ) {
      return yield* error("source-identity-conflict", command);
    }
    if (
      command.githubIntakeSequence < state.githubIntakeSequence ||
      command.sourceUpdatedAt < state.sourceUpdatedAt
    ) {
      return yield* error("source-state-conflict", command);
    }
    if (command.githubIntakeSequence === state.githubIntakeSequence) {
      return sameSourceMutation(state, command)
        ? []
        : yield* error("source-state-conflict", command);
    }

    if (command.type === "agentControl.task.sourceGate.refresh") {
      if (
        (command.sourceGate !== "identity-invalid" &&
          command.sourceGate !== "source-missing" &&
          normalizedTaskSourceGate(command.sourceSnapshot) !== command.sourceGate) ||
        (state.status === "needs-attention" &&
          state.sourceGate === "source-missing" &&
          command.sourceGate === "eligible") ||
        (state.status === "needs-attention" &&
          state.sourceGate === "identity-invalid" &&
          command.sourceGate !== "identity-invalid")
      ) {
        return yield* error("source-state-conflict", command);
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

    if (command.type === "agentControl.task.markNeedsAttention") {
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
            source: state.source,
            previousStatus: state.status,
            previousSourceGate: state.sourceGate,
            sourceGate: command.sourceGate,
            sourceUpdatedAt: command.sourceUpdatedAt,
            githubIntakeSequence: command.githubIntakeSequence,
            sourceSnapshot: command.sourceSnapshot,
            markedAt: occurredAt,
          },
        },
      ];
    }

    if (
      state.status !== "needs-attention" ||
      state.sourceGate !== "source-missing" ||
      command.sourceGate !== "eligible" ||
      normalizedTaskSourceGate(command.sourceSnapshot) !== "eligible"
    ) {
      return yield* error("source-state-conflict", command);
    }
    return [
      {
        eventId,
        type: "agentControl.task.sourceMissingRecovered",
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
          previousStatus: "needs-attention",
          previousSourceGate: "source-missing",
          status: "candidate",
          sourceGate: "eligible",
          sourceUpdatedAt: command.sourceUpdatedAt,
          githubIntakeSequence: command.githubIntakeSequence,
          sourceSnapshot: command.sourceSnapshot,
          recoveredAt: occurredAt,
        },
      },
    ];
  },
);
