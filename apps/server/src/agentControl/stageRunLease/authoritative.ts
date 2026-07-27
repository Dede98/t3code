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

const leasePositionKey = (state: AgentControlStageRunLeaseState) =>
  [
    state.projectId,
    state.taskId,
    state.stageRunId,
    state.attemptId,
    String(state.taskRevision),
    String(state.githubIntakeSequence),
    state.sourceIdentityFingerprint,
  ].join("\0");

/**
 * Reconstructs the union of every lease event stream and projection. This is
 * deliberately history-first: no projection-only/latest-wins selection is
 * permitted for a stage position.
 */
export const loadAuthoritativeLeaseHistory = Effect.fn("loadAuthoritativeLeaseHistory")(function* (
  events: Pick<AgentControlStageRunLeaseEventStoreShape, "readGlobal">,
  states: Pick<AgentControlStageRunLeaseStateRepositoryShape, "listAll">,
): Effect.fn.Return<
  ReadonlyArray<AgentControlStageRunLeaseState>,
  | AgentControlStageRunLeaseEventStoreError
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError
> {
  const foldedByLease = new Map<string, AgentControlStageRunLeaseState>();
  let afterSequence = 0;
  while (true) {
    const page = yield* events.readGlobal(afterSequence, PAGE_SIZE);
    if (page.length === 0) break;
    for (const event of page) {
      if (event.sequence <= afterSequence) return yield* corrupt();
      afterSequence = event.sequence;
      const prior = foldedByLease.get(event.aggregateId) ?? null;
      if (
        event.aggregateKind !== "stage-run-lease" ||
        event.payload.leaseId !== event.aggregateId ||
        event.streamVersion !== (prior?.revision ?? 0) + 1
      ) {
        return yield* corrupt();
      }
      foldedByLease.set(
        event.aggregateId,
        yield* projectAgentControlStageRunLeaseEvent(prior, event),
      );
    }
  }

  const projectedEntries = yield* states.listAll;
  if (projectedEntries.some((entry) => entry._tag === "Corrupt")) return yield* corrupt();
  const projected = projectedEntries.flatMap((entry) =>
    entry._tag === "Valid" ? [entry.state] : [],
  );
  if (foldedByLease.size !== projected.length) return yield* corrupt();
  const projectedById = new Map(projected.map((state) => [state.leaseId, state] as const));
  const positions = new Map<string, string>();
  const authoritative: Array<AgentControlStageRunLeaseState> = [];
  for (const folded of foldedByLease.values()) {
    const validated = yield* validateAgentControlStageRunLeaseState(folded);
    const projection = projectedById.get(validated.leaseId);
    if (projection === undefined || !sameLeaseState(validated, projection)) {
      return yield* corrupt();
    }
    projectedById.delete(validated.leaseId);
    const key = leasePositionKey(validated);
    const prior = positions.get(key);
    if (prior !== undefined && prior !== validated.leaseId) return yield* corrupt();
    positions.set(key, validated.leaseId);
    authoritative.push(validated);
  }
  if (projectedById.size !== 0) return yield* corrupt();
  return authoritative;
});

export const loadAuthoritativeLeaseHistoryForStagePosition = Effect.fn(
  "loadAuthoritativeLeaseHistoryForStagePosition",
)(function* (
  position: {
    readonly projectId: ProjectId;
    readonly taskId: AgentControlTaskId;
    readonly stageRunId: AgentControlStageRunState["stageRunId"];
    readonly attemptId: AgentControlStageRunState["attemptId"];
    readonly taskRevision: number;
    readonly githubIntakeSequence: number;
    readonly sourceIdentityFingerprint: string;
  },
  events: Pick<AgentControlStageRunLeaseEventStoreShape, "readGlobal">,
  states: Pick<AgentControlStageRunLeaseStateRepositoryShape, "listAll">,
) {
  const history = yield* loadAuthoritativeLeaseHistory(events, states);
  return history.filter(
    (state) =>
      state.projectId === position.projectId &&
      state.taskId === position.taskId &&
      state.stageRunId === position.stageRunId &&
      state.attemptId === position.attemptId &&
      state.taskRevision === position.taskRevision &&
      state.githubIntakeSequence === position.githubIntakeSequence &&
      state.sourceIdentityFingerprint === position.sourceIdentityFingerprint,
  );
});

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
