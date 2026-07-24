import {
  AgentControlProjectionCorruptError,
  type AgentControlWorktreeEvent,
  type AgentControlWorktreeReservationId,
  type AgentControlWorktreeReservationState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type { AgentControlRepositoryError } from "../Errors.ts";
import { AGENT_CONTROL_WORKTREE_PROJECTOR } from "./invariant.ts";
import { projectAgentControlWorktreeEvent } from "./projector.ts";
import type {
  AgentControlWorktreeEventStoreError,
  AgentControlWorktreeEventStoreShape,
} from "./Services/AgentControlWorktreeEventStore.ts";
import type { AgentControlWorktreeStateRepositoryShape } from "./Services/AgentControlWorktreeStateRepository.ts";

const PAGE_SIZE = 500;
const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_WORKTREE_PROJECTOR,
  });

const sameState = (
  left: AgentControlWorktreeReservationState,
  right: AgentControlWorktreeReservationState,
) =>
  left.schemaVersion === right.schemaVersion &&
  left.reservationId === right.reservationId &&
  left.projectId === right.projectId &&
  left.taskId === right.taskId &&
  left.taskRevision === right.taskRevision &&
  left.githubIntakeSequence === right.githubIntakeSequence &&
  left.sourceIdentityFingerprint === right.sourceIdentityFingerprint &&
  left.stageRunId === right.stageRunId &&
  left.attemptId === right.attemptId &&
  left.leaseId === right.leaseId &&
  left.fenceToken === right.fenceToken &&
  left.repository.repositoryNodeId === right.repository.repositoryNodeId &&
  left.repository.nameWithOwner === right.repository.nameWithOwner &&
  left.repository.canonicalKey === right.repository.canonicalKey &&
  left.repositoryWorkspace === right.repositoryWorkspace &&
  left.repositoryCommonDir === right.repositoryCommonDir &&
  left.baseRef === right.baseRef &&
  left.baseCommitSha === right.baseCommitSha &&
  left.branchName === right.branchName &&
  left.internalWorktreePath === right.internalWorktreePath &&
  left.status === right.status &&
  left.headCommitSha === right.headCommitSha &&
  left.attentionCode === right.attentionCode &&
  left.createdAt === right.createdAt &&
  left.updatedAt === right.updatedAt &&
  left.revision === right.revision &&
  left.sequence === right.sequence;

export const loadAuthoritativeWorktreeReservation = Effect.fn(
  "loadAuthoritativeWorktreeReservation",
)(function* (
  reservationId: AgentControlWorktreeReservationId,
  events: Pick<AgentControlWorktreeEventStoreShape, "readStream">,
  states: Pick<AgentControlWorktreeStateRepositoryShape, "get">,
): Effect.fn.Return<
  Option.Option<{
    readonly state: AgentControlWorktreeReservationState;
    readonly events: ReadonlyArray<AgentControlWorktreeEvent>;
    readonly statesByVersion: ReadonlyArray<AgentControlWorktreeReservationState>;
  }>,
  | AgentControlWorktreeEventStoreError
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError
> {
  const stream: Array<AgentControlWorktreeEvent> = [];
  const statesByVersion: Array<AgentControlWorktreeReservationState> = [];
  let state: AgentControlWorktreeReservationState | null = null;
  let after = 0;
  while (true) {
    const page = yield* events.readStream(reservationId, after, PAGE_SIZE);
    if (page.length === 0) break;
    for (const event of page) {
      if (event.aggregateId !== reservationId || event.streamVersion !== after + 1) {
        return yield* corrupt();
      }
      state = yield* projectAgentControlWorktreeEvent(state, event);
      stream.push(event);
      statesByVersion.push(state);
      after = event.streamVersion;
    }
  }
  const projected = yield* states.get(reservationId);
  if (state === null) {
    if (Option.isSome(projected)) return yield* corrupt();
    return Option.none();
  }
  if (Option.isNone(projected) || !sameState(state, projected.value)) {
    return yield* corrupt();
  }
  return Option.some({ state, events: stream, statesByVersion });
});
