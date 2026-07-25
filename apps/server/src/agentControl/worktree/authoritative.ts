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

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_WORKTREE_PROJECTOR,
  });

export const sameAgentControlWorktreeReservationState = (
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
  left.repository.remoteName === right.repository.remoteName &&
  left.repository.remoteUrl === right.repository.remoteUrl &&
  left.repository.defaultRemoteRef === right.repository.defaultRemoteRef &&
  left.repository.commonDirDevice === right.repository.commonDirDevice &&
  left.repository.commonDirInode === right.repository.commonDirInode &&
  left.repositoryWorkspace === right.repositoryWorkspace &&
  left.repositoryCommonDir === right.repositoryCommonDir &&
  left.baseRef === right.baseRef &&
  left.baseCommitSha === right.baseCommitSha &&
  left.branchName === right.branchName &&
  left.internalWorktreePath === right.internalWorktreePath &&
  left.targetGenerationId === right.targetGenerationId &&
  left.worktreeRootDevice === right.worktreeRootDevice &&
  left.worktreeRootInode === right.worktreeRootInode &&
  left.worktreeParentDevice === right.worktreeParentDevice &&
  left.worktreeParentInode === right.worktreeParentInode &&
  left.materializationPhase === right.materializationPhase &&
  left.gitCreatedDevice === right.gitCreatedDevice &&
  left.gitCreatedInode === right.gitCreatedInode &&
  left.gitCreatedGitDir === right.gitCreatedGitDir &&
  left.markedOwnershipFingerprint === right.markedOwnershipFingerprint &&
  left.status === right.status &&
  left.headCommitSha === right.headCommitSha &&
  left.ownershipFingerprint === right.ownershipFingerprint &&
  left.verifiedAt === right.verifiedAt &&
  left.reservedAt === right.reservedAt &&
  left.attentionCode === right.attentionCode &&
  left.createdAt === right.createdAt &&
  left.updatedAt === right.updatedAt &&
  left.revision === right.revision &&
  left.sequence === right.sequence;

export const foldAuthoritativeWorktreeReservationStream = Effect.fn(
  "foldAuthoritativeWorktreeReservationStream",
)(function* (
  reservationId: AgentControlWorktreeReservationId,
  events: Pick<AgentControlWorktreeEventStoreShape, "readStreamSnapshot">,
): Effect.fn.Return<
  Option.Option<{
    readonly state: AgentControlWorktreeReservationState;
    readonly events: ReadonlyArray<AgentControlWorktreeEvent>;
    readonly statesByVersion: ReadonlyArray<AgentControlWorktreeReservationState>;
  }>,
  AgentControlWorktreeEventStoreError | AgentControlProjectionCorruptError
> {
  const stream: Array<AgentControlWorktreeEvent> = [];
  const statesByVersion: Array<AgentControlWorktreeReservationState> = [];
  let state: AgentControlWorktreeReservationState | null = null;
  const validated = yield* events.readStreamSnapshot(reservationId);
  for (const event of validated) {
    if (event.aggregateId !== reservationId || event.streamVersion !== stream.length + 1) {
      return yield* corrupt();
    }
    state = yield* projectAgentControlWorktreeEvent(state, event);
    stream.push(event);
    statesByVersion.push(state);
  }
  return state === null ? Option.none() : Option.some({ state, events: stream, statesByVersion });
});

export const loadAuthoritativeWorktreeReservation = Effect.fn(
  "loadAuthoritativeWorktreeReservation",
)(function* (
  reservationId: AgentControlWorktreeReservationId,
  events: Pick<AgentControlWorktreeEventStoreShape, "readStreamSnapshot">,
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
  const folded = yield* foldAuthoritativeWorktreeReservationStream(reservationId, events);
  const projected = yield* states.get(reservationId);
  if (Option.isNone(folded)) {
    if (Option.isSome(projected)) return yield* corrupt();
    return Option.none();
  }
  if (
    Option.isNone(projected) ||
    !sameAgentControlWorktreeReservationState(folded.value.state, projected.value)
  ) {
    return yield* corrupt();
  }
  return folded;
});
