import {
  AgentControlProjectionCorruptError,
  type AgentControlTaskEvent,
  type AgentControlTaskState,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { AgentControlRepositoryError, AgentControlTaskEventStoreError } from "../Errors.ts";
import { projectAgentControlTaskEvent } from "./projector.ts";
import type { AgentControlTaskEventStoreShape } from "./Services/AgentControlTaskEventStore.ts";
import type { AgentControlTaskStateRepositoryShape } from "./Services/AgentControlTaskStateRepository.ts";

const PAGE_SIZE = 500;
const PROJECTOR = "agent-control-task-v1";

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: PROJECTOR,
  });

export const sameAgentControlTaskState = (
  left: AgentControlTaskState,
  right: AgentControlTaskState,
) =>
  left.schemaVersion === right.schemaVersion &&
  left.taskId === right.taskId &&
  left.source.projectId === right.source.projectId &&
  left.source.repositoryNodeId === right.source.repositoryNodeId &&
  left.source.issueNodeId === right.source.issueNodeId &&
  left.source.issueNumber === right.source.issueNumber &&
  left.source.issueUrl === right.source.issueUrl &&
  left.status === right.status &&
  left.sourceGate === right.sourceGate &&
  left.stage === right.stage &&
  left.sourceUpdatedAt === right.sourceUpdatedAt &&
  left.githubIntakeSequence === right.githubIntakeSequence &&
  left.sourceSnapshot.repositoryNodeId === right.sourceSnapshot.repositoryNodeId &&
  left.sourceSnapshot.issueNodeId === right.sourceSnapshot.issueNodeId &&
  left.sourceSnapshot.number === right.sourceSnapshot.number &&
  left.sourceSnapshot.url === right.sourceSnapshot.url &&
  left.sourceSnapshot.state === right.sourceSnapshot.state &&
  left.sourceSnapshot.title === right.sourceSnapshot.title &&
  left.sourceSnapshot.body === right.sourceSnapshot.body &&
  left.sourceSnapshot.contentTrust === right.sourceSnapshot.contentTrust &&
  left.sourceSnapshot.updatedAt === right.sourceSnapshot.updatedAt &&
  left.sourceSnapshot.timelineComplete === right.sourceSnapshot.timelineComplete &&
  left.sourceSnapshot.ready === right.sourceSnapshot.ready &&
  left.sourceSnapshot.paused === right.sourceSnapshot.paused &&
  left.sourceSnapshot.eligible === right.sourceSnapshot.eligible &&
  left.sourceSnapshot.eligibilityReason === right.sourceSnapshot.eligibilityReason &&
  left.createdAt === right.createdAt &&
  left.updatedAt === right.updatedAt &&
  left.revision === right.revision &&
  left.sequence === right.sequence;

const identityKey = (state: AgentControlTaskState) =>
  [state.source.projectId, state.source.repositoryNodeId, state.source.issueNodeId].join("\0");

const numberKey = (state: AgentControlTaskState) =>
  [state.source.projectId, state.source.repositoryNodeId, String(state.source.issueNumber)].join(
    "\0",
  );

/**
 * Reconstructs every task stream in global-sequence pages inside the caller's
 * SQLite snapshot, then compares the complete authoritative project history
 * with every projection row. Event-only, projection-only, malformed, gapped,
 * and competing task identities all fail closed.
 */
export const loadAuthoritativeTaskProjectHistory = Effect.fn("loadAuthoritativeTaskProjectHistory")(
  function* (
    projectId: ProjectId,
    events: Pick<AgentControlTaskEventStoreShape, "readGlobal">,
    states: Pick<AgentControlTaskStateRepositoryShape, "listProject">,
  ): Effect.fn.Return<
    ReadonlyArray<AgentControlTaskState>,
    | AgentControlTaskEventStoreError
    | AgentControlRepositoryError
    | AgentControlProjectionCorruptError
  > {
    const foldedByTask = new Map<string, AgentControlTaskState>();
    let afterSequence = 0;
    while (true) {
      const page = yield* events.readGlobal(afterSequence, PAGE_SIZE);
      if (page.length === 0) break;
      for (const event of page) {
        if (event.sequence <= afterSequence) return yield* corrupt();
        afterSequence = event.sequence;
        const prior = foldedByTask.get(event.aggregateId) ?? null;
        if (
          event.aggregateKind !== "task" ||
          event.payload.taskId !== event.aggregateId ||
          event.streamVersion !== (prior?.revision ?? 0) + 1
        ) {
          return yield* corrupt();
        }
        foldedByTask.set(
          event.aggregateId,
          yield* projectAgentControlTaskEvent(prior, event as AgentControlTaskEvent),
        );
      }
    }

    const authoritative = [...foldedByTask.values()].filter(
      (state) => state.source.projectId === projectId,
    );
    const projectedEntries = yield* states.listProject(projectId);
    if (projectedEntries.some((entry) => entry._tag === "Corrupt")) return yield* corrupt();
    const projected = projectedEntries.flatMap((entry) =>
      entry._tag === "Valid" ? [entry.state] : [],
    );
    if (authoritative.length !== projected.length) return yield* corrupt();

    const projectedById = new Map(projected.map((state) => [state.taskId, state] as const));
    const identityOwners = new Map<string, string>();
    const numberOwners = new Map<string, string>();
    for (const state of authoritative) {
      const projection = projectedById.get(state.taskId);
      if (projection === undefined || !sameAgentControlTaskState(state, projection)) {
        return yield* corrupt();
      }
      projectedById.delete(state.taskId);
      for (const [key, owners] of [
        [identityKey(state), identityOwners],
        [numberKey(state), numberOwners],
      ] as const) {
        const prior = owners.get(key);
        if (prior !== undefined && prior !== state.taskId) return yield* corrupt();
        owners.set(key, state.taskId);
      }
    }
    if (projectedById.size !== 0) return yield* corrupt();
    return authoritative;
  },
);
