import {
  type AgentControlGithubEvent,
  type AgentControlGithubIntakeState,
  AgentControlProjectionCorruptError,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

export const AGENT_CONTROL_GITHUB_PROJECTOR = "agent-control-github-intake-v1";

export const createDefaultGithubIntakeState = (
  projectId: ProjectId,
): AgentControlGithubIntakeState => ({
  schemaVersion: 1,
  projectId,
  config: null,
  cursor: null,
  pollStatus: {
    status: "disabled",
    attemptedAt: null,
    completedAt: null,
    errorCode: null,
  },
  revision: 0,
  sequence: 0,
  updatedAt: null,
});

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_GITHUB_PROJECTOR,
  });

const sameRepository = (
  left: { readonly repositoryNodeId: string; readonly nameWithOwner: string },
  right: { readonly repositoryNodeId: string; readonly nameWithOwner: string },
) =>
  left.repositoryNodeId === right.repositoryNodeId &&
  left.nameWithOwner.toLocaleLowerCase("en-US") === right.nameWithOwner.toLocaleLowerCase("en-US");

export const projectGithubIntakeEvent = Effect.fn("projectGithubIntakeEvent")(function* (
  state: AgentControlGithubIntakeState,
  event: AgentControlGithubEvent,
) {
  if (
    event.aggregateKind !== "github-intake" ||
    event.aggregateId !== state.projectId ||
    event.payload.projectId !== state.projectId ||
    event.commandId !== event.correlationId ||
    event.causationEventId !== null ||
    event.streamVersion !== state.revision + 1 ||
    event.sequence <= state.sequence
  ) {
    return yield* corrupt();
  }

  switch (event.type) {
    case "agentControl.github.config.set":
      if (event.payload.configuredAt !== event.occurredAt) return yield* corrupt();
      return {
        schemaVersion: 1,
        projectId: state.projectId,
        config: {
          schemaVersion: 1,
          projectId: state.projectId,
          settings: event.payload.settings,
          repository: event.payload.repository,
          revision: event.streamVersion,
          sequence: event.sequence,
          updatedAt: event.occurredAt,
        },
        cursor: null,
        pollStatus: {
          status: "not-polled",
          attemptedAt: null,
          completedAt: null,
          errorCode: null,
        },
        revision: event.streamVersion,
        sequence: event.sequence,
        updatedAt: event.occurredAt,
      } satisfies AgentControlGithubIntakeState;
    case "agentControl.github.config.cleared":
      if (event.payload.clearedAt !== event.occurredAt) return yield* corrupt();
      return {
        schemaVersion: 1,
        projectId: state.projectId,
        config: null,
        cursor: null,
        pollStatus: {
          status: "disabled",
          attemptedAt: null,
          completedAt: null,
          errorCode: null,
        },
        revision: event.streamVersion,
        sequence: event.sequence,
        updatedAt: event.occurredAt,
      } satisfies AgentControlGithubIntakeState;
    case "agentControl.github.poll.succeeded": {
      if (
        state.config === null ||
        !sameRepository(state.config.repository, event.payload.repository) ||
        event.payload.completedAt !== event.occurredAt ||
        event.payload.cursor.lastSuccessfulPollAt !== event.payload.attemptedAt
      ) {
        return yield* corrupt();
      }
      return {
        ...state,
        cursor: event.payload.cursor,
        pollStatus: {
          status: "success",
          attemptedAt: event.payload.attemptedAt,
          completedAt: event.payload.completedAt,
          errorCode: null,
          issueCount: event.payload.issues.length,
        },
        revision: event.streamVersion,
        sequence: event.sequence,
        updatedAt: event.occurredAt,
        config: {
          ...state.config,
          revision: event.streamVersion,
          sequence: event.sequence,
          updatedAt: event.occurredAt,
        },
      } satisfies AgentControlGithubIntakeState;
    }
    case "agentControl.github.poll.failed":
      if (state.config === null || event.payload.completedAt !== event.occurredAt) {
        return yield* corrupt();
      }
      return {
        ...state,
        cursor: event.payload.invalidateCursor ? null : state.cursor,
        pollStatus: {
          status: "needs-attention",
          attemptedAt: event.payload.attemptedAt,
          completedAt: event.payload.completedAt,
          errorCode: event.payload.errorCode,
        },
        revision: event.streamVersion,
        sequence: event.sequence,
        updatedAt: event.occurredAt,
        config: {
          ...state.config,
          revision: event.streamVersion,
          sequence: event.sequence,
          updatedAt: event.occurredAt,
        },
      } satisfies AgentControlGithubIntakeState;
  }
});
