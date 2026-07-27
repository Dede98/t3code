import type { AgentControlThreadMaterializeCommand } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "../agentControl/controlledThreadReservation/identity.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../agentControl/stageRun/identity.ts";
import { deriveAgentControlStageRunLeaseId } from "../agentControl/stageRunLease/identity.ts";
import { OrchestrationCommandIdentityConflictError } from "./Errors.ts";

/**
 * Reconstructs every derivable materialization identifier from the complete
 * command identity. This validation is shared by decision, first persistence,
 * and receipt replay so persisted or caller-supplied identifiers never become
 * authority merely because they agree with each other.
 */
export const validateAgentControlThreadMaterializationCommandIdentity = Effect.fn(
  "validateAgentControlThreadMaterializationCommandIdentity",
)(function* (command: AgentControlThreadMaterializeCommand) {
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId: command.projectId,
    taskId: command.taskId,
    taskRevision: command.taskRevision,
    githubIntakeSequence: command.githubIntakeSequence,
    sourceIdentityFingerprint: command.sourceIdentityFingerprint,
    stageKind: command.stageKind,
    stageOrdinal: command.stageOrdinal,
  });
  const attemptId = yield* deriveAgentControlAttemptId(stageRunId, command.attemptOrdinal);
  const leaseId = yield* deriveAgentControlStageRunLeaseId({
    projectId: command.projectId,
    taskId: command.taskId,
  });
  const stableIdentity = {
    projectId: command.projectId,
    taskId: command.taskId,
    taskRevision: command.taskRevision,
    githubIntakeSequence: command.githubIntakeSequence,
    sourceIdentityFingerprint: command.sourceIdentityFingerprint,
    stageRunId,
    attemptId,
    roleId: command.roleId,
    stageKind: command.stageKind,
    stageOrdinal: command.stageOrdinal,
    attemptOrdinal: command.attemptOrdinal,
  } as const;
  const controlledThreadReservationId =
    yield* deriveAgentControlControlledThreadReservationId(stableIdentity);
  const threadId = yield* deriveAgentControlReservedThreadId(stableIdentity);

  if (
    command.stageRunId !== stageRunId ||
    command.attemptId !== attemptId ||
    command.leaseId !== leaseId ||
    command.controlledThreadReservationId !== controlledThreadReservationId ||
    command.threadId !== threadId
  ) {
    return yield* new OrchestrationCommandIdentityConflictError({
      commandId: command.commandId,
      commandType: command.type,
    });
  }
});
