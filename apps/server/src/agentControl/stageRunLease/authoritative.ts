import {
  AgentControlProjectionCorruptError,
  type AgentControlStageRunLeaseEvent,
  type AgentControlStageRunLeaseId,
  type AgentControlStageRunLeaseState,
  type AgentControlStageRunState,
  type AgentControlTaskId,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type {
  AgentControlRepositoryError,
  AgentControlStageRunEventStoreError,
  AgentControlStageRunLeaseEventStoreError,
} from "../Errors.ts";
import { projectAgentControlStageRunEvent } from "../stageRun/projector.ts";
import type { AgentControlStageRunEventStoreShape } from "../stageRun/Services/AgentControlStageRunEventStore.ts";
import type { AgentControlStageRunStateRepositoryShape } from "../stageRun/Services/AgentControlStageRunStateRepository.ts";
import {
  AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR,
  validateAgentControlStageRunLeaseState,
} from "./invariant.ts";
import { projectAgentControlStageRunLeaseEvent } from "./projector.ts";
import type { AgentControlStageRunLeaseEventStoreShape } from "./Services/AgentControlStageRunLeaseEventStore.ts";
import type { AgentControlStageRunLeaseStateRepositoryShape } from "./Services/AgentControlStageRunLeaseStateRepository.ts";

const PAGE_SIZE = 500;

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR,
  });

const sameLeaseState = (
  left: AgentControlStageRunLeaseState,
  right: AgentControlStageRunLeaseState,
) =>
  left.schemaVersion === right.schemaVersion &&
  left.leaseId === right.leaseId &&
  left.projectId === right.projectId &&
  left.taskId === right.taskId &&
  left.stageRunId === right.stageRunId &&
  left.attemptId === right.attemptId &&
  left.holderId === right.holderId &&
  left.fenceToken === right.fenceToken &&
  left.status === right.status &&
  left.taskRevision === right.taskRevision &&
  left.githubIntakeSequence === right.githubIntakeSequence &&
  left.sourceIdentityFingerprint === right.sourceIdentityFingerprint &&
  left.acquiredAt === right.acquiredAt &&
  left.renewedAt === right.renewedAt &&
  left.expiresAt === right.expiresAt &&
  left.releasedAt === right.releasedAt &&
  left.revision === right.revision &&
  left.sequence === right.sequence;

const sameStageRunState = (left: AgentControlStageRunState, right: AgentControlStageRunState) =>
  left.schemaVersion === right.schemaVersion &&
  left.stageRunId === right.stageRunId &&
  left.projectId === right.projectId &&
  left.taskId === right.taskId &&
  left.attemptId === right.attemptId &&
  left.roleId === right.roleId &&
  left.stageKind === right.stageKind &&
  left.stageOrdinal === right.stageOrdinal &&
  left.attemptOrdinal === right.attemptOrdinal &&
  left.status === right.status &&
  left.taskRevision === right.taskRevision &&
  left.githubIntakeSequence === right.githubIntakeSequence &&
  left.sourceIdentityFingerprint === right.sourceIdentityFingerprint &&
  left.createdAt === right.createdAt &&
  left.updatedAt === right.updatedAt &&
  left.revision === right.revision &&
  left.sequence === right.sequence;

export interface AuthoritativeLeaseState {
  readonly state: AgentControlStageRunLeaseState;
  readonly events: ReadonlyArray<AgentControlStageRunLeaseEvent>;
  readonly statesByVersion: ReadonlyArray<AgentControlStageRunLeaseState>;
}

/**
 * The sole lease fold used by mutation and accepted-receipt replay. It reads by
 * stream version in bounded pages and treats the event stream as authoritative.
 */
export const loadAuthoritativeLeaseState = Effect.fn("loadAuthoritativeLeaseState")(function* (
  leaseId: AgentControlStageRunLeaseId,
  events: Pick<AgentControlStageRunLeaseEventStoreShape, "readStream">,
  states: Pick<AgentControlStageRunLeaseStateRepositoryShape, "get">,
): Effect.fn.Return<
  Option.Option<AuthoritativeLeaseState>,
  | AgentControlStageRunLeaseEventStoreError
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError
> {
  const stream: Array<AgentControlStageRunLeaseEvent> = [];
  const statesByVersion: Array<AgentControlStageRunLeaseState> = [];
  let folded: AgentControlStageRunLeaseState | null = null;
  let afterStreamVersion = 0;

  while (true) {
    const page = yield* events.readStream(leaseId, afterStreamVersion, PAGE_SIZE);
    if (page.length === 0) break;
    for (const event of page) {
      if (
        event.aggregateKind !== "stage-run-lease" ||
        event.aggregateId !== leaseId ||
        event.streamVersion !== afterStreamVersion + 1
      ) {
        return yield* corrupt();
      }
      folded = yield* projectAgentControlStageRunLeaseEvent(folded, event);
      afterStreamVersion = event.streamVersion;
      stream.push(event);
      statesByVersion.push(folded);
    }
  }

  const projection = yield* states.get(leaseId);
  if (folded === null) {
    if (Option.isSome(projection)) return yield* corrupt();
    return Option.none();
  }
  if (Option.isNone(projection)) return yield* corrupt();
  const validated = yield* validateAgentControlStageRunLeaseState(folded);
  if (!sameLeaseState(validated, projection.value)) return yield* corrupt();
  return Option.some({ state: validated, events: stream, statesByVersion });
});

/**
 * Admission companion to the lease reader. Every relevant prepared Stage Run
 * must exist in both its event history and projection, including older runs.
 */
export const loadAuthoritativeInitialStageRunHistory = Effect.fn(
  "loadAuthoritativeInitialStageRunHistory",
)(function* (
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  events: Pick<AgentControlStageRunEventStoreShape, "readGlobal">,
  states: Pick<AgentControlStageRunStateRepositoryShape, "listInitialForTask">,
): Effect.fn.Return<
  ReadonlyArray<AgentControlStageRunState>,
  | AgentControlStageRunEventStoreError
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError
> {
  const rebuilt: Array<AgentControlStageRunState> = [];
  let afterSequence = 0;
  while (true) {
    const page = yield* events.readGlobal(afterSequence, PAGE_SIZE);
    if (page.length === 0) break;
    for (const event of page) {
      if (event.sequence <= afterSequence) return yield* corrupt();
      afterSequence = event.sequence;
      if (event.payload.projectId !== projectId || event.payload.taskId !== taskId) continue;
      if (event.streamVersion !== 1) return yield* corrupt();
      rebuilt.push(yield* projectAgentControlStageRunEvent(null, event));
    }
  }

  const projected = yield* states.listInitialForTask(projectId, taskId);
  if (rebuilt.length !== projected.length) return yield* corrupt();
  const byId = new Map(projected.map((state) => [state.stageRunId, state] as const));
  for (const state of rebuilt) {
    const projection = byId.get(state.stageRunId);
    if (projection === undefined || !sameStageRunState(state, projection)) {
      return yield* corrupt();
    }
    byId.delete(state.stageRunId);
  }
  if (byId.size !== 0) return yield* corrupt();
  return rebuilt.sort(
    (left, right) =>
      right.taskRevision - left.taskRevision ||
      right.githubIntakeSequence - left.githubIntakeSequence ||
      right.stageOrdinal - left.stageOrdinal ||
      left.stageRunId.localeCompare(right.stageRunId),
  );
});
