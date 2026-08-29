import {
  AgentControlProjectionCorruptError,
  type AgentControlStageRunLeaseState,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import {
  AGENT_CONTROL_INITIAL_ATTEMPT_ORDINAL,
  AGENT_CONTROL_INITIAL_STAGE_KIND,
  AGENT_CONTROL_INITIAL_STAGE_ORDINAL,
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../stageRun/identity.ts";
import { deriveAgentControlStageRunLeaseId } from "./identity.ts";

export const AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR = "agent-control-stage-run-lease-v1";
const CANONICAL_SHA256 = /^[0-9a-f]{64}$/;
const CANONICAL_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR,
  });

export const canonicalTimestampMillis = (value: string): number | null => {
  if (!CANONICAL_TIMESTAMP.test(value)) return null;
  const parsed = DateTime.make(value);
  if (Option.isNone(parsed) || DateTime.formatIso(parsed.value) !== value) return null;
  return DateTime.toEpochMillis(parsed.value);
};

export const validateAgentControlStageRunLeaseState = Effect.fn(
  "validateAgentControlStageRunLeaseState",
)(function* (
  state: AgentControlStageRunLeaseState,
): Effect.fn.Return<AgentControlStageRunLeaseState, AgentControlProjectionCorruptError> {
  const acquiredAt = canonicalTimestampMillis(state.acquiredAt);
  const renewedAt = canonicalTimestampMillis(state.renewedAt);
  const expiresAt = canonicalTimestampMillis(state.expiresAt);
  const releasedAt = state.releasedAt === null ? null : canonicalTimestampMillis(state.releasedAt);
  if (
    state.schemaVersion !== 1 ||
    state.fenceToken < 1 ||
    state.revision < 1 ||
    state.sequence < 1 ||
    acquiredAt === null ||
    renewedAt === null ||
    expiresAt === null ||
    acquiredAt > renewedAt ||
    expiresAt <= renewedAt ||
    !CANONICAL_SHA256.test(state.sourceIdentityFingerprint) ||
    (state.status === "reserved" && state.releasedAt !== null) ||
    (state.status === "released" && (releasedAt === null || releasedAt < renewedAt))
  ) {
    return yield* corrupt();
  }

  const leaseId = yield* deriveAgentControlStageRunLeaseId(state);
  const planningStageRunId = yield* deriveAgentControlStageRunId({
    projectId: state.projectId,
    taskId: state.taskId,
    taskRevision: state.taskRevision,
    githubIntakeSequence: state.githubIntakeSequence,
    sourceIdentityFingerprint: state.sourceIdentityFingerprint,
    stageKind: AGENT_CONTROL_INITIAL_STAGE_KIND,
    stageOrdinal: AGENT_CONTROL_INITIAL_STAGE_ORDINAL,
  });
  const planningAttemptId = yield* deriveAgentControlAttemptId(
    planningStageRunId,
    AGENT_CONTROL_INITIAL_ATTEMPT_ORDINAL,
  );
  const implementationStageRunId = yield* deriveAgentControlStageRunId({
    projectId: state.projectId,
    taskId: state.taskId,
    taskRevision: state.taskRevision,
    githubIntakeSequence: state.githubIntakeSequence,
    sourceIdentityFingerprint: state.sourceIdentityFingerprint,
    stageKind: "implementation",
    stageOrdinal: 2,
  });
  const implementationAttemptId = yield* deriveAgentControlAttemptId(implementationStageRunId, 1);
  const verificationStageRunId = yield* deriveAgentControlStageRunId({
    projectId: state.projectId,
    taskId: state.taskId,
    taskRevision: state.taskRevision,
    githubIntakeSequence: state.githubIntakeSequence,
    sourceIdentityFingerprint: state.sourceIdentityFingerprint,
    stageKind: "verification",
    stageOrdinal: 3,
  });
  const verificationAttemptId = yield* deriveAgentControlAttemptId(verificationStageRunId, 1);
  const verification =
    state.stageRunId === verificationStageRunId && state.attemptId === verificationAttemptId;
  if (
    state.leaseId !== leaseId ||
    (verification && state.fenceToken < 3) ||
    !(
      (state.stageRunId === planningStageRunId && state.attemptId === planningAttemptId) ||
      (state.stageRunId === implementationStageRunId &&
        state.attemptId === implementationAttemptId) ||
      verification
    )
  ) {
    return yield* corrupt();
  }
  return state;
});
