import {
  AgentControlProjectionCorruptError,
  type AgentControlWorktreeReservationState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { deriveAgentControlWorktreeReservationId } from "./identity.ts";
import { canonicalTimestampMillis } from "../stageRunLease/invariant.ts";

export const AGENT_CONTROL_WORKTREE_PROJECTOR = "agent-control-worktree-reservation-v1";
const SHA256 = /^[0-9a-f]{64}$/;
const GIT_OBJECT_ID = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const BRANCH = /^t3auto\/issue-[1-9][0-9]*-[a-z0-9]+(?:-[a-z0-9]+)*$/;
const isAbsoluteLocalPath = (value: string) => value.startsWith("/");

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_WORKTREE_PROJECTOR,
  });

export const validateAgentControlWorktreeReservationState = Effect.fn(
  "validateAgentControlWorktreeReservationState",
)(function* (
  state: AgentControlWorktreeReservationState,
): Effect.fn.Return<AgentControlWorktreeReservationState, AgentControlProjectionCorruptError> {
  if (
    state.schemaVersion !== 1 ||
    state.revision < 1 ||
    state.sequence < 1 ||
    state.fenceToken < 1 ||
    !SHA256.test(state.sourceIdentityFingerprint) ||
    !GIT_OBJECT_ID.test(state.baseCommitSha) ||
    (state.headCommitSha !== null && !GIT_OBJECT_ID.test(state.headCommitSha)) ||
    !BRANCH.test(state.branchName) ||
    !isAbsoluteLocalPath(state.repositoryWorkspace) ||
    !isAbsoluteLocalPath(state.repositoryCommonDir) ||
    !isAbsoluteLocalPath(state.internalWorktreePath) ||
    canonicalTimestampMillis(state.createdAt) === null ||
    canonicalTimestampMillis(state.updatedAt) === null ||
    canonicalTimestampMillis(state.updatedAt)! < canonicalTimestampMillis(state.createdAt)! ||
    (state.status === "ready" &&
      (state.headCommitSha !== state.baseCommitSha ||
        state.ownershipFingerprint === null ||
        !SHA256.test(state.ownershipFingerprint) ||
        state.verifiedAt === null ||
        canonicalTimestampMillis(state.verifiedAt) === null ||
        canonicalTimestampMillis(state.verifiedAt)! < canonicalTimestampMillis(state.createdAt)! ||
        canonicalTimestampMillis(state.verifiedAt)! >
          canonicalTimestampMillis(state.updatedAt)!)) ||
    (state.status !== "ready" &&
      (state.headCommitSha !== null ||
        state.ownershipFingerprint !== null ||
        state.verifiedAt !== null)) ||
    (state.status === "needs-attention"
      ? state.attentionCode === null
      : state.attentionCode !== null)
  ) {
    return yield* corrupt();
  }
  const reservationId = yield* deriveAgentControlWorktreeReservationId({
    projectId: state.projectId,
    taskId: state.taskId,
    stageRunId: state.stageRunId,
    attemptId: state.attemptId,
    leaseId: state.leaseId,
    fenceToken: state.fenceToken,
    repositoryIdentity: {
      repositoryNodeId: state.repository.repositoryNodeId,
      canonicalKey: state.repository.canonicalKey,
    },
    baseCommitSha: state.baseCommitSha,
  });
  if (reservationId !== state.reservationId) return yield* corrupt();
  return state;
});
