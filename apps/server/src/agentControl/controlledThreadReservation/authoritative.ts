import {
  AgentControlProjectionCorruptError,
  type AgentControlControlledThreadReservationEvent,
  type AgentControlControlledThreadReservationId,
  type AgentControlControlledThreadReservationState,
  type AgentControlTaskId,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type {
  AgentControlControlledThreadReservationEventStoreError,
  AgentControlRepositoryError,
} from "../Errors.ts";
import {
  AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR,
  validateAgentControlControlledThreadReservationState,
} from "./invariant.ts";
import { projectAgentControlControlledThreadReservationEvent } from "./projector.ts";
import type { AgentControlControlledThreadReservationEventStoreShape } from "./Services/AgentControlControlledThreadReservationEventStore.ts";
import type { AgentControlControlledThreadReservationStateRepositoryShape } from "./Services/AgentControlControlledThreadReservationStateRepository.ts";

const PAGE_SIZE = 500;
const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR,
  });

export const sameAgentControlControlledThreadReservationState = (
  left: AgentControlControlledThreadReservationState,
  right: AgentControlControlledThreadReservationState,
) =>
  left.schemaVersion === right.schemaVersion &&
  left.controlledThreadReservationId === right.controlledThreadReservationId &&
  left.threadId === right.threadId &&
  left.projectId === right.projectId &&
  left.taskId === right.taskId &&
  left.taskRevision === right.taskRevision &&
  left.githubIntakeSequence === right.githubIntakeSequence &&
  left.sourceIdentityFingerprint === right.sourceIdentityFingerprint &&
  left.stageRunId === right.stageRunId &&
  left.attemptId === right.attemptId &&
  left.roleId === right.roleId &&
  left.stageKind === right.stageKind &&
  left.stageOrdinal === right.stageOrdinal &&
  left.attemptOrdinal === right.attemptOrdinal &&
  left.leaseId === right.leaseId &&
  left.fenceToken === right.fenceToken &&
  left.worktreeReservationId === right.worktreeReservationId &&
  left.status === right.status &&
  left.revision === right.revision &&
  left.sequence === right.sequence &&
  left.preparedAt === right.preparedAt &&
  (left.status === "prepared"
    ? right.status === "prepared"
    : right.status !== "prepared" &&
      left.coordinatorCommandId === right.coordinatorCommandId &&
      left.coordinatorCommandFingerprint === right.coordinatorCommandFingerprint &&
      left.materializingTransitionCommandId === right.materializingTransitionCommandId &&
      left.materializationCommandId === right.materializationCommandId &&
      left.materializationCommandFingerprint === right.materializationCommandFingerprint &&
      left.leaseHolderId === right.leaseHolderId &&
      left.materializingAt === right.materializingAt &&
      (left.status === "materializing"
        ? right.status === "materializing"
        : right.status === "bound" &&
          left.boundTransitionCommandId === right.boundTransitionCommandId &&
          left.orchestrationResultSequence === right.orchestrationResultSequence &&
          left.materializedAt === right.materializedAt &&
          left.boundAt === right.boundAt));

const semanticKey = (state: AgentControlControlledThreadReservationState) =>
  [
    state.projectId,
    state.taskId,
    state.stageRunId,
    state.attemptId,
    state.roleId,
    String(state.stageOrdinal),
    String(state.attemptOrdinal),
  ].join("\0");

export const loadAuthoritativeControlledThreadReservation = Effect.fn(
  "loadAuthoritativeControlledThreadReservation",
)(function* (
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  events: Pick<AgentControlControlledThreadReservationEventStoreShape, "readStream">,
  states: Pick<AgentControlControlledThreadReservationStateRepositoryShape, "get">,
): Effect.fn.Return<
  Option.Option<AgentControlControlledThreadReservationState>,
  | AgentControlControlledThreadReservationEventStoreError
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError
> {
  const folded = yield* foldAuthoritativeControlledThreadReservationStream(
    controlledThreadReservationId,
    events,
  );
  const projected = yield* states.get(controlledThreadReservationId);
  if (Option.isNone(folded)) {
    if (Option.isSome(projected)) return yield* corrupt();
    return Option.none();
  }
  if (
    Option.isNone(projected) ||
    !sameAgentControlControlledThreadReservationState(folded.value, projected.value)
  ) {
    return yield* corrupt();
  }
  return Option.some(yield* validateAgentControlControlledThreadReservationState(folded.value));
});

export const foldAuthoritativeControlledThreadReservationStream = Effect.fn(
  "foldAuthoritativeControlledThreadReservationStream",
)(function* (
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  events: Pick<AgentControlControlledThreadReservationEventStoreShape, "readStream">,
): Effect.fn.Return<
  Option.Option<AgentControlControlledThreadReservationState>,
  AgentControlControlledThreadReservationEventStoreError | AgentControlProjectionCorruptError
> {
  let after = 0;
  let folded: AgentControlControlledThreadReservationState | null = null;
  while (true) {
    const page = yield* events.readStream(controlledThreadReservationId, after, PAGE_SIZE);
    if (page.length === 0) break;
    for (const event of page) {
      if (
        event.aggregateId !== controlledThreadReservationId ||
        event.streamVersion !== after + 1
      ) {
        return yield* corrupt();
      }
      folded = yield* projectAgentControlControlledThreadReservationEvent(folded, event);
      after = event.streamVersion;
    }
  }
  return folded === null
    ? Option.none()
    : Option.some(yield* validateAgentControlControlledThreadReservationState(folded));
});

/**
 * Enumerates both the task's complete event history and projection history.
 * No latest-wins selection is allowed for a semantic position.
 */
export const loadAuthoritativeControlledThreadReservationTaskHistory = Effect.fn(
  "loadAuthoritativeControlledThreadReservationTaskHistory",
)(function* (
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  events: Pick<AgentControlControlledThreadReservationEventStoreShape, "readTask">,
  states: Pick<AgentControlControlledThreadReservationStateRepositoryShape, "listTask">,
): Effect.fn.Return<
  ReadonlyArray<AgentControlControlledThreadReservationState>,
  | AgentControlControlledThreadReservationEventStoreError
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError
> {
  const rebuiltById = new Map<string, AgentControlControlledThreadReservationState>();
  let afterSequence = 0;
  while (true) {
    const page = yield* events.readTask(projectId, taskId, afterSequence, PAGE_SIZE);
    if (page.length === 0) break;
    for (const event of page) {
      if (event.sequence <= afterSequence) return yield* corrupt();
      afterSequence = event.sequence;
      if (event.payload.projectId !== projectId || event.payload.taskId !== taskId) continue;
      const current = rebuiltById.get(event.aggregateId) ?? null;
      rebuiltById.set(
        event.aggregateId,
        yield* projectAgentControlControlledThreadReservationEvent(current, event),
      );
    }
  }

  const rebuilt = [...rebuiltById.values()];
  const projected = yield* states.listTask(projectId, taskId);
  if (rebuilt.length !== projected.length) return yield* corrupt();
  const projectedById = new Map(
    projected.map((state) => [state.controlledThreadReservationId, state] as const),
  );
  for (const state of rebuilt) {
    const projection = projectedById.get(state.controlledThreadReservationId);
    if (
      projection === undefined ||
      !sameAgentControlControlledThreadReservationState(state, projection)
    ) {
      return yield* corrupt();
    }
    projectedById.delete(state.controlledThreadReservationId);
  }
  if (projectedById.size !== 0) return yield* corrupt();

  const positions = new Map<string, AgentControlControlledThreadReservationState>();
  for (const state of rebuilt) {
    const key = semanticKey(state);
    const prior = positions.get(key);
    if (
      prior !== undefined &&
      prior.controlledThreadReservationId !== state.controlledThreadReservationId
    ) {
      return yield* corrupt();
    }
    positions.set(key, state);
  }

  return rebuilt.toSorted(
    (left, right) =>
      left.taskRevision - right.taskRevision ||
      left.githubIntakeSequence - right.githubIntakeSequence ||
      left.stageOrdinal - right.stageOrdinal ||
      left.attemptOrdinal - right.attemptOrdinal ||
      left.controlledThreadReservationId.localeCompare(right.controlledThreadReservationId),
  );
});

export const collectControlledThreadReservationEventsForTask = Effect.fn(
  "collectControlledThreadReservationEventsForTask",
)(function* (
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  events: Pick<AgentControlControlledThreadReservationEventStoreShape, "readGlobal">,
): Effect.fn.Return<
  ReadonlyArray<AgentControlControlledThreadReservationEvent>,
  AgentControlControlledThreadReservationEventStoreError | AgentControlProjectionCorruptError
> {
  const collected: Array<AgentControlControlledThreadReservationEvent> = [];
  let afterSequence = 0;
  while (true) {
    const page = yield* events.readGlobal(afterSequence, PAGE_SIZE);
    if (page.length === 0) break;
    for (const event of page) {
      if (event.sequence <= afterSequence) return yield* corrupt();
      afterSequence = event.sequence;
      if (event.payload.projectId === projectId && event.payload.taskId === taskId) {
        collected.push(event);
      }
    }
  }
  return collected;
});
