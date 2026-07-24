import {
  AgentControlProjectionCorruptError,
  type AgentControlStageRunState,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import {
  AGENT_CONTROL_INITIAL_ATTEMPT_ORDINAL,
  AGENT_CONTROL_INITIAL_STAGE_KIND,
  AGENT_CONTROL_INITIAL_STAGE_ORDINAL,
  AGENT_CONTROL_PLANNING_ROLE_ID,
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "./identity.ts";

const PROJECTOR = "agent-control-stage-run-v1";
const CANONICAL_SHA256 = /^[0-9a-f]{64}$/;
const CANONICAL_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

const isCanonicalTimestamp = (value: string) => {
  if (!CANONICAL_TIMESTAMP.test(value)) return false;
  const parsed = DateTime.make(value);
  return Option.isSome(parsed) && DateTime.formatIso(parsed.value) === value;
};

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: PROJECTOR,
  });

export const validateInitialAgentControlStageRunState = Effect.fn(
  "validateInitialAgentControlStageRunState",
)(function* (
  state: AgentControlStageRunState,
): Effect.fn.Return<AgentControlStageRunState, AgentControlProjectionCorruptError> {
  if (
    state.schemaVersion !== 1 ||
    state.stageKind !== AGENT_CONTROL_INITIAL_STAGE_KIND ||
    state.roleId !== AGENT_CONTROL_PLANNING_ROLE_ID ||
    state.stageOrdinal !== AGENT_CONTROL_INITIAL_STAGE_ORDINAL ||
    state.attemptOrdinal !== AGENT_CONTROL_INITIAL_ATTEMPT_ORDINAL ||
    state.status !== "prepared" ||
    state.revision !== 1 ||
    state.sequence <= 0 ||
    state.createdAt !== state.updatedAt ||
    !isCanonicalTimestamp(state.createdAt) ||
    !CANONICAL_SHA256.test(state.sourceIdentityFingerprint)
  ) {
    return yield* corrupt();
  }
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId: state.projectId,
    taskId: state.taskId,
    taskRevision: state.taskRevision,
    githubIntakeSequence: state.githubIntakeSequence,
    sourceIdentityFingerprint: state.sourceIdentityFingerprint,
    stageKind: state.stageKind,
    stageOrdinal: state.stageOrdinal,
  });
  const attemptId = yield* deriveAgentControlAttemptId(stageRunId, state.attemptOrdinal);
  if (state.stageRunId !== stageRunId || state.attemptId !== attemptId) {
    return yield* corrupt();
  }
  return state;
});
