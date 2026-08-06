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

const canonicalTimestampMillis = (value: string) => {
  const parsed = DateTime.make(value);
  return Option.isSome(parsed) && DateTime.formatIso(parsed.value) === value
    ? DateTime.toEpochMillis(parsed.value)
    : null;
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
  const validated = yield* validateAgentControlStageRunState(state);
  if (validated.status !== "prepared") return yield* corrupt();
  return validated;
});

export const validateAgentControlStageRunState = Effect.fn("validateAgentControlStageRunState")(
  function* (
    state: AgentControlStageRunState,
  ): Effect.fn.Return<AgentControlStageRunState, AgentControlProjectionCorruptError> {
    const planning =
      state.stageKind === AGENT_CONTROL_INITIAL_STAGE_KIND &&
      state.roleId === AGENT_CONTROL_PLANNING_ROLE_ID &&
      state.stageOrdinal === AGENT_CONTROL_INITIAL_STAGE_ORDINAL &&
      state.attemptOrdinal === AGENT_CONTROL_INITIAL_ATTEMPT_ORDINAL;
    const implementation =
      state.stageKind === "implementation" &&
      state.roleId === "implementer" &&
      state.stageOrdinal === 2 &&
      state.attemptOrdinal === 1;
    const verification =
      state.stageKind === "verification" &&
      state.roleId === "verifier" &&
      state.stageOrdinal === 3 &&
      state.attemptOrdinal === 1;
    const expectedRevision = state.status === "prepared" ? 1 : state.status === "running" ? 2 : 3;
    if (
      state.schemaVersion !== 1 ||
      (!planning && !implementation && !verification) ||
      (verification && state.status !== "prepared") ||
      (state.status !== "prepared" &&
        state.status !== "running" &&
        state.status !== "succeeded" &&
        state.status !== "failed" &&
        state.status !== "cancelled") ||
      state.revision !== expectedRevision ||
      state.sequence <= 0 ||
      (state.status === "prepared" && state.createdAt !== state.updatedAt) ||
      !isCanonicalTimestamp(state.createdAt) ||
      !isCanonicalTimestamp(state.updatedAt) ||
      canonicalTimestampMillis(state.updatedAt)! < canonicalTimestampMillis(state.createdAt)! ||
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
  },
);
